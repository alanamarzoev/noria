#[macro_use]
extern crate clap;
extern crate futures;

use noria::{ControllerHandle, ZookeeperAuthority, DataType, Builder, LocalAuthority, ReuseConfigType, SyncHandle, Handle};
use std::net::IpAddr;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};
use futures::future::Future;
use std::sync::Arc;
use zookeeper::ZooKeeper;
use noria::SyncControllerHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tokio::prelude::*;
use tokio::executor::Executor;

pub struct Backend {
    g: Handle<ZookeeperAuthority>,
    rt: tokio::runtime::Runtime,
}

#[derive(PartialEq)]
enum PopulateType {
    Before,
    After,
    NoPopulate,
}

pub enum DataflowType {
    Write,
    Read,
}

impl Backend {
    pub fn new(partial: bool, _shard: bool, reuse: &str, dftype: DataflowType) -> Backend {
        match dftype {
            DataflowType::Read => {
                println!("in backend new. read");
                let zk_address = "127.0.0.1:2181/read";
                let mut rt = tokio::runtime::Runtime::new().unwrap();
                let authority = Arc::new(ZookeeperAuthority::new(zk_address).unwrap());
                let mut cb = Builder::default();
                let log = noria::logger_pls();
                let blender_log = log.clone();
                cb.set_reuse(ReuseConfigType::NoReuse);
                let g = rt.block_on(cb.start(authority)).unwrap();
                cb.log_with(blender_log);
                Backend { g, rt }
            },
            DataflowType::Write => {
                println!("in backend new. write");
                let zk_address = "127.0.0.1:2181/write";
                let mut rt = tokio::runtime::Runtime::new().unwrap();

                let authority = Arc::new(ZookeeperAuthority::new(zk_address).unwrap());
                let mut cb = Builder::default();
                cb.set_fwd_addr(true);
                let log = noria::logger_pls();
                let blender_log = log.clone();

                cb.set_reuse(ReuseConfigType::NoReuse);

                let g = rt.block_on(cb.start(authority)).unwrap();

                cb.log_with(blender_log);
                Backend { g, rt }
            }
        }
    }

    pub fn populate<T>(&mut self, name: &'static str, mut records: Vec<Vec<DataType>>, mut db: SyncControllerHandle<ZookeeperAuthority, T>)
    where T : tokio::executor::Executor
    {
        println!("in populate");
        let get_table = move |b: &mut SyncControllerHandle<_, _>, n| loop {
            match b.table(n) {
                Ok(v) => return v.into_sync(),
                Err(_) => {
                    panic!("tried to get table via controller handle and failed. should not happen!");
                    // thread::sleep(Duration::from_millis(50));
                    // *b = SyncControllerHandle::from_zk("127.0.0.1:2181/write", executor.clone())
                    //     .unwrap();
                }
            }
        };

        // Get mutators and getter.
        let mut table = get_table(&mut db, name);
        for r in records.drain(..) {
            println!("inserting");
            table
                .insert(r)
                .unwrap();
        }
    }

    fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.set_security_config(config);
        println!("SET SECURITY CONFIG");
        //self.g.on_worker(|w| w.set_security_config(config)).unwrap();
    }

    fn migrate<T>(&mut self, schema_file: &str, query_file: Option<&str>, db: &mut SyncControllerHandle<ZookeeperAuthority, T>) -> Result<(), String>
    where T : tokio::executor::Executor
    {
        use std::io::Read;

        // Read schema file
        println!("opening schema file: {:#?}", schema_file);
        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();
        sf.read_to_string(&mut s).unwrap();

        let mut rs = s.clone();
        s.clear();

        // Read query file
        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s);
            }
        }
        println!("after open");
        println!("result of install recipe: {:#?}", db.extend_recipe(&rs).unwrap());
        Ok(())
    }

    // fn set_fwd_addr<T>(&mut self, fwdaddr: &str, db: &mut SyncControllerHandle<ZookeeperAuthority, T>) -> Result<(), String>
    // where T : tokio::executor::Executor
    // {
    //     println!("H1");
    //     db.set_fwd_addr(fwdaddr);
    //     println!("H2");
    //     Ok(())
    // }
}


// Write policy test
// Policy: don't allow articles to be written when aid is 42.

fn main() {
    use clap::{App, Arg};
    let args = App::new("piazza")
        .version("0.1")
        .about("Write policy test benchmark")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("noria-benchmarks/write-policies/schema.sql")
                .help("Schema file"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("noria-benchmarks/write-policies/queries.sql")
                .help("Query file"),
        )
        .arg(
            Arg::with_name("wpolicies")
                .long("wpolicies")
                .required(true)
                .default_value("noria-benchmarks/write-policies/write-policies.json")
                .help("Write policies"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("shard")
                .long("shard")
                .help("Enable sharding"),
        )
        .arg(
            Arg::with_name("partial")
                .long("partial")
                .help("Enable partial materialization"),
        )
        .arg(
            Arg::with_name("populate")
                .long("populate")
                .default_value("nopopulate")
                .possible_values(&["after", "before", "nopopulate"])
                .help("Populate app with randomly generated data"),
        )
        .get_matches();

    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let wploc = args.value_of("wpolicies").unwrap();
    let gloc = args.value_of("graph");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let reuse = args.value_of("reuse").unwrap();
    let populate = args.value_of("populate").unwrap_or("nopopulate");

    println!("Initializing read and write dataflows and configuring them...");

    // create both the read and write dataflows
    println!("CREATING WDF");
    let mut write_df = Backend::new(partial, shard, reuse, DataflowType::Write);
    println!("CREATING WDF handle");

    let wexecutor = write_df.rt.executor();
    let mut write_db = SyncControllerHandle::from_zk("127.0.0.1:2181/write", wexecutor).unwrap();


    println!("MIG");
    write_df.migrate(sloc, None, &mut write_db).unwrap();
    println!("SET SEC CONFIG");
    write_df.set_security_config(wploc);
    println!("MIG");
    write_df.migrate(sloc, Some(qloc), &mut write_db).unwrap();

    // thread::sleep(time::Duration::from_millis(100));
    //
    // let mut read_df = Backend::new(partial, shard, reuse, DataflowType::Read);
    // let rexecutor = read_df.rt.executor();
    // let mut read_db = SyncControllerHandle::from_zk("127.0.0.1:2181/read", rexecutor, None).unwrap();
    //
    // read_df.migrate(sloc, Some(qloc), &mut read_db).unwrap();
    //
    // thread::sleep(time::Duration::from_millis(100));
    //
    // // populate write DF w posts
    // println!("Populating posts...");
    // let mut records : Vec<Vec<DataType>> = Vec::new();
    // for i in 0..10 {
    //     let pid = i.into();
    //     let author = i.into();
    //     let cid = 0.into();
    //     let content = "".into();
    //     let anon = 1.into();
    //     if i != 0 {
    //         let private = 0.into();
    //         records.push(vec![pid, cid, author, content, private, anon]);
    //     } else {
    //         let private = 0.into();
    //         records.push(vec![pid, cid, author, content, private, anon]);
    //     }
    // }
    //
    // // populate WDF with records
    // write_df.populate("Post", records, write_db);
    //
    // let executor = read_df.rt.executor();
    // let get_view = move |b: &mut SyncControllerHandle<_, _>, n| loop {
    //     match b.view(n) {
    //         Ok(v) => return v.into_sync(),
    //         Err(_) => {
    //             thread::sleep(Duration::from_millis(50));
    //             *b = SyncControllerHandle::from_zk("127.0.0.1:2181/read", executor.clone(), None)
    //                 .unwrap();
    //         }
    //     }
    // };
    //
    // let mut awvc = get_view(&mut read_db, "posts");
    // let res = awvc.lookup(&[0.into()], true).unwrap();
    // println!("result: {:#?}", res);

}
