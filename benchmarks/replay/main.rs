#[macro_use]
extern crate clap;
extern crate distributary;
extern crate hdrhistogram;
extern crate itertools;
extern crate rand;
extern crate zookeeper;

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{App, Arg};
use hdrhistogram::Histogram;
use itertools::Itertools;
use zookeeper::ZooKeeper;

use distributary::{ControllerBuilder, ControllerHandle, DataType, DurabilityMode,
                   PersistenceParameters, ZookeeperAuthority};

// If we .batch_put a huge amount of rows we'll end up with a deadlock when the base
// domains fill up their TCP buffers trying to send ACKs (which the batch putter
// isn't reading yet, since it's still busy sending).
const BATCH_SIZE: usize = 10000;

const RECIPE: &str = "
CREATE TABLE TableRow (id int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, PRIMARY KEY(id));
QUERY ReadRow: SELECT * FROM TableRow WHERE id = ?;
";

const SECONDARY_RECIPE: &str = "
CREATE TABLE TableRow (id int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, PRIMARY KEY(id));
QUERY ReadRow: SELECT * FROM TableRow WHERE id = ?;
QUERY query_c1: SELECT * FROM TableRow WHERE c1 = ?;
QUERY query_c2: SELECT * FROM TableRow WHERE c2 = ?;
QUERY query_c3: SELECT * FROM TableRow WHERE c3 = ?;
QUERY query_c4: SELECT * FROM TableRow WHERE c4 = ?;
QUERY query_c5: SELECT * FROM TableRow WHERE c5 = ?;
QUERY query_c6: SELECT * FROM TableRow WHERE c6 = ?;
QUERY query_c7: SELECT * FROM TableRow WHERE c7 = ?;
QUERY query_c8: SELECT * FROM TableRow WHERE c8 = ?;
QUERY query_c9: SELECT * FROM TableRow WHERE c9 = ?;
";

fn build_graph(
    authority: Arc<ZookeeperAuthority>,
    persistence: PersistenceParameters,
    verbose: bool,
) -> ControllerHandle<ZookeeperAuthority> {
    let mut builder = ControllerBuilder::default();
    if verbose {
        builder.log_with(distributary::logger_pls());
    }

    builder.set_persistence(persistence);
    builder.set_sharding(None);
    builder.set_read_threads(1);
    builder.set_worker_threads(1);
    builder.build(authority)
}

fn populate(g: &mut ControllerHandle<ZookeeperAuthority>, rows: i64) {
    let mut mutator = g.get_mutator("TableRow").unwrap();

    (0..rows)
        .map(|i| {
            let row: Vec<DataType> = vec![i.into(); 10];
            row
        })
        .chunks(BATCH_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let rs: Vec<Vec<DataType>> = chunk.collect();
            mutator.multi_put(rs).unwrap();
        });
}

// Synchronously read `reads` times, where each read should trigger a full replay from the base.
fn perform_reads(
    g: &mut ControllerHandle<ZookeeperAuthority>,
    reads: i64,
    rows: i64,
    use_secondary: bool,
    verbose: bool,
) {
    if verbose {
        eprintln!("Done populating state, now reading articles...");
    }

    let mut hist = Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap();
    let mut rng = rand::thread_rng();
    let row_ids = rand::seq::sample_iter(&mut rng, 0..rows, reads as usize).unwrap();
    if use_secondary {
        perform_secondary_reads(g, &mut hist, row_ids);
    } else {
        perform_primary_reads(g, &mut hist, row_ids);
    }

    println!("# read {} of {} rows", reads, rows);
    println!("read\t50\t{:.2}\t(all µs)", hist.value_at_quantile(0.5));
    println!("read\t95\t{:.2}\t(all µs)", hist.value_at_quantile(0.95));
    println!("read\t99\t{:.2}\t(all µs)", hist.value_at_quantile(0.99));
    println!("read\t100\t{:.2}\t(all µs)", hist.max());
}

// Reads every row with the primary key index.
fn perform_primary_reads(
    g: &mut ControllerHandle<ZookeeperAuthority>,
    hist: &mut Histogram<u64>,
    row_ids: Vec<i64>,
) {
    let mut getter = g.get_getter("ReadRow").unwrap();

    for i in row_ids {
        let id: DataType = DataType::BigInt(i);
        let start = Instant::now();
        let rs = getter.lookup(&[id], true).unwrap();
        let elapsed = start.elapsed();
        let us = elapsed.as_secs() * 1_000_000 + elapsed.subsec_nanos() as u64 / 1_000;
        assert_eq!(rs.len(), 1);
        for j in 0..10 {
            assert_eq!(DataType::BigInt(i), rs[0][j]);
        }

        if hist.record(us).is_err() {
            let m = hist.high();
            hist.record(m).unwrap();
        }
    }
}

// Reads each row from one of the secondary indices.
fn perform_secondary_reads(
    g: &mut ControllerHandle<ZookeeperAuthority>,
    hist: &mut Histogram<u64>,
    row_ids: Vec<i64>,
) {
    let indices = 10;
    let mut getters: Vec<_> = (1..indices)
        .map(|i| g.get_getter(&format!("query_c{}", i)).unwrap())
        .collect();

    for i in row_ids {
        let id: DataType = DataType::BigInt(i);
        let start = Instant::now();
        // Pick an arbitrary secondary index to use:
        let getter = &mut getters[i as usize % (indices - 1)];
        let rs = getter.lookup(&[id], true).unwrap();
        let elapsed = start.elapsed();
        let us = elapsed.as_secs() * 1_000_000 + elapsed.subsec_nanos() as u64 / 1_000;
        assert_eq!(rs.len(), 1);
        for j in 0..10 {
            assert_eq!(DataType::BigInt(i), rs[0][j]);
        }

        if hist.record(us).is_err() {
            let m = hist.high();
            hist.record(m).unwrap();
        }
    }
}

fn clear_zookeeper(address: &str) {
    let zk = ZooKeeper::connect(address, Duration::from_secs(1), |_| {}).unwrap();
    let _ = zk.delete("/state", None);
}

fn main() {
    let args = App::new("replay")
        .version("0.1")
        .about("Benchmarks the latency of full replays in a user-curated news aggregator")
        .arg(
            Arg::with_name("rows")
                .long("rows")
                .value_name("N")
                .default_value("100000")
                .help("Number of rows to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("reads")
                .long("reads")
                .default_value("10000")
                .help("Number of rows to read while benchmarking"),
        )
        .arg(
            Arg::with_name("secondary-indices")
                .long("secondary-indices")
                .help("Read from secondary indices."),
        )
        .arg(
            Arg::with_name("log-dir")
                .long("log-dir")
                .takes_value(true)
                .help("Absolute path to the directory where the log files will be written."),
        )
        .arg(
            Arg::with_name("durability")
                .long("durability")
                .takes_value(false)
                .help("Enable durability for Base nodes"),
        )
        .arg(
            Arg::with_name("retain-logs-on-exit")
                .long("retain-logs-on-exit")
                .takes_value(false)
                .requires("durability")
                .help("Do not delete the base node logs on exit."),
        )
        .arg(
            Arg::with_name("use-existing-data")
                .long("use-existing-data")
                .requires("retain-logs-on-exit")
                .takes_value(false)
                .help("Skips pre-population and instead uses already persisted data."),
        )
        .arg(
            Arg::with_name("write-batch-size")
                .long("write-batch-size")
                .takes_value(true)
                .default_value("512")
                .help("Size of batches processed at base nodes."),
        )
        .arg(
            Arg::with_name("zookeeper-address")
                .long("zookeeper-address")
                .takes_value(true)
                .default_value("127.0.0.1:2181/replay")
                .help("ZookeeperAuthority address"),
        )
        .arg(
            Arg::with_name("flush-timeout")
                .long("flush-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to wait before processing a merged packet, in nanoseconds."),
        )
        .arg(
            Arg::with_name("persistence-threads")
                .long("persistence-threads")
                .takes_value(true)
                .default_value("1")
                .help("Number of background threads used by PersistentState."),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let reads = value_t_or_exit!(args, "reads", i64);
    let rows = value_t_or_exit!(args, "rows", i64);
    assert!(reads < rows);

    let verbose = args.is_present("verbose");
    let durable = args.is_present("durability");
    let use_secondary = args.is_present("secondary-indices");
    let flush_ns = value_t_or_exit!(args, "flush-timeout", u32);

    let mut persistence = PersistenceParameters::default();
    persistence.flush_timeout = Duration::new(0, flush_ns);
    persistence.persistence_threads = value_t_or_exit!(args, "persistence-threads", i32);
    persistence.queue_capacity = value_t_or_exit!(args, "write-batch-size", usize);
    persistence.log_prefix = "replay".to_string();
    persistence.mode = if durable {
        DurabilityMode::Permanent
    } else {
        DurabilityMode::MemoryOnly
    };

    persistence.log_dir = args.value_of("log-dir")
        .and_then(|p| Some(PathBuf::from(p)));

    let zk_address = args.value_of("zookeeper-address").unwrap();
    let authority = Arc::new(ZookeeperAuthority::new(zk_address));

    if !args.is_present("use-existing-data") {
        clear_zookeeper(zk_address);
        let mut g = build_graph(authority.clone(), persistence.clone(), verbose);
        if use_secondary {
            g.install_recipe(SECONDARY_RECIPE.to_owned()).unwrap();
        } else {
            g.install_recipe(RECIPE.to_owned()).unwrap();
        }

        if verbose {
            eprintln!("Populating with {} rows", rows);
        }

        // Prepopulate with n rows:
        populate(&mut g, rows);

        // In memory-only mode we don't want to recover, just read right away:
        if !durable {
            perform_reads(&mut g, reads, rows, use_secondary, verbose);
            return;
        }
    }

    // Recover the previous graph and perform reads:
    let mut g = build_graph(authority, persistence, verbose);
    // Flush disk cache:
    Command::new("sync")
        .spawn()
        .expect("Failed clearing disk buffers");
    perform_reads(&mut g, reads, rows, use_secondary, verbose);

    // Remove any log/database files:
    if !args.is_present("retain-logs-on-exit") {
        fs::remove_dir_all("replay-TableRow-0.db").unwrap();
    }
}
