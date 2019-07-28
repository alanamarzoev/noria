#[macro_use]
extern crate clap;

use noria::{ControllerHandle, ZookeeperAuthority, DataType, Builder, LocalAuthority, ReuseConfigType, SyncHandle, Handle};
use std::net::IpAddr;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};
use futures::future::Future;
use std::sync::Arc;
use zookeeper::ZooKeeper;


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
                // cb.disable_partial();
                // if !partial {
                    // cb.disable_partial();
                // }
                //let g = cb.start(authority).wait().unwrap();
                let log = noria::logger_pls();
                let blender_log = log.clone();

                cb.set_reuse(ReuseConfigType::NoReuse);
                //let g = cb.start(authority).wait().unwrap();

                let g = rt.block_on(cb.start(authority)).unwrap();

                // match reuse {
                //     "finkelstein" => cb.set_reuse(ReuseConfigType::Finkelstein),
                //     "full" => cb.set_reuse(ReuseConfigType::Full),
                //     "noreuse" => cb.set_reuse(ReuseConfigType::NoReuse),
                //     "relaxed" => cb.set_reuse(ReuseConfigType::Relaxed),
                //     _ => panic!("reuse configuration not supported"),
                // }

                cb.log_with(blender_log);

                // let g = cb.start_simple().unwrap();

                Backend { g, rt }
            },
            DataflowType::Write => {
                println!("in backend new. write");
                let zk_address = "127.0.0.1:2181/write";
                let mut rt = tokio::runtime::Runtime::new().unwrap();

                let authority = Arc::new(ZookeeperAuthority::new(zk_address).unwrap());
                let mut cb = Builder::default();
                // cb.disable_partial();
                // if !partial {
                    // cb.disable_partial();
                // }
                println!("woohoo");
                //let g = cb.start(authority).wait().unwrap();
                let log = noria::logger_pls();
                let blender_log = log.clone();

                cb.set_reuse(ReuseConfigType::NoReuse);
                //let g = cb.start(authority).wait().unwrap();

                let g = rt.block_on(cb.start(authority)).unwrap();

                // match reuse {
                //     "finkelstein" => cb.set_reuse(ReuseConfigType::Finkelstein),
                //     "full" => cb.set_reuse(ReuseConfigType::Full),
                //     "noreuse" => cb.set_reuse(ReuseConfigType::NoReuse),
                //     "relaxed" => cb.set_reuse(ReuseConfigType::Relaxed),
                //     _ => panic!("reuse configuration not supported"),
                // }

                cb.log_with(blender_log);

                // let g = cb.start_simple().unwrap();

                Backend { g, rt }
            }
        }
    }

    pub fn populate(&mut self, name: &'static str, mut records: Vec<Vec<DataType>>) {
        let mut mutator = self.g.table(name).wait().unwrap().into_sync();
        for r in records.drain(..) {
            println!("inserting record: {:#?}", r);
            mutator.insert(r).unwrap();
        }
    }

    fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.set_security_config(config);
        //self.g.on_worker(|w| w.set_security_config(config)).unwrap();
    }
    
    fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
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
    
        // Install recipe
        self.g.install_recipe(&rs).unwrap();
    
        Ok(())
    }
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


    println!("Initializing database schema...");

    // create both the read and write dataflows
    let mut write_df = Backend::new(partial, shard, reuse, DataflowType::Write);
    // let mut read_df = Backend::new(partial, shard, reuse, DataflowType::Read);

    // set schema for both the read and write dataflows
    write_df.migrate(sloc, Some(qloc)).unwrap();
    // read_df.migrate(sloc, Some(qloc)).unwrap();
    //
    // // set write policies
    // write_df.set_security_config(wploc);
    // write_df.migrate(sloc, Some(qloc)).unwrap();
    //
    // println!("Populating posts...");
    // let mut records = Vec::new();
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
    //         let private = 1.into();
    //         records.push(vec![pid, cid, author, content, private, anon]);
    //     }
    // }
    //
    // write_df.populate("Post", records);
    //
    // let leaf = "posts".to_string();
    // let mut getter = read_df.g.view(&leaf).unwrap().into_sync();
    // let mut getter2 = write_df.g.view(&leaf).unwrap().into_sync();
    // for author in 0..10 {
    //     let res = getter.lookup(&[author.into()], false).unwrap();
    //     let res2 = getter2.lookup(&[author.into()], false).unwrap();
    //     println!("READ DF result for id: {:?} --> {:#?}", author, res);
    //     println!("WRITE DF result for id: {:?} --> {:#?}", author, res2);
    // }

    // insert articles
    // let aid1 = 42;
    // let aid2 = 43;
    // let title = "I love Soup";
    // let url = "https://pdos.csail.mit.edu";
    //
    // let mut articles_to_insert = Vec::new();
    // articles_to_insert.push(vec![aid1.into(), title.into(), url.into()]);
    // articles_to_insert.push(vec![aid2.into(), title.into(), url.into()]);
    //
    // write_df.populate("Article", articles_to_insert);
    //
    // // insert votes
    //
    // let mut votes = Vec::new();
    // votes.push(vec![aid1.into(), title.into()]);
    // votes.push(vec![aid2.into(), title.into()]);
    //
    // write_df.populate("Vote", votes);

}



// let mut wp_addr = "127.0.0.1:2181/wp";
// let mut rp_addr = "127.0.0.1:2181/rp";
//
// let mut wp = ControllerHandle::from_zk(wp_addr).unwrap();
// let mut rp = ControllerHandle::from_zk(rp_addr).unwrap();
//
//
// // install the same schema in both the read and write DF for simplicity
// wp.install_recipe("
//   CREATE TABLE Article (aid int, title varchar(255), url text, PRIMARY KEY(aid));
//   CREATE TABLE Vote (aid int, uid int);
// ");
//
// rp.install_recipe("
//   CREATE TABLE Article (aid int, title varchar(255), url text, PRIMARY KEY(aid));
//   CREATE TABLE Vote (aid int, uid int);
// ");
//
// // write handles
// let mut article = wp.table("Article").unwrap();
// let mut vote = wp.table("Vote").unwrap();
//
// // introduce "illegal" article
// let aid1 = 42;
// let title = "I love Soup";
// let url = "https://pdos.csail.mit.edu";
// article
//     .insert(vec![aid1.into(), title.into(), url.into()])
//     .unwrap();
//
// // vote for illegal article
// vote.insert(vec![aid1.into(), title.into()]).unwrap();
//
// // introduce "legal" article
// let aid2 = 43;
// article
//     .insert(vec![aid2.into(), title.into(), url.into()])
//     .unwrap();
//
// // vote for legal article
// vote.insert(vec![aid2.into(), title.into()]).unwrap();
//
// // we can also declare views that we want want to query
// wp.extend_recipe("
//     VoteCount: \
//         SELECT Vote.aid, COUNT(uid) AS votes \
//     FROM Vote GROUP BY Vote.aid;
//         QUERY ArticleWithVoteCount: \
//     SELECT Article.aid, title, url, VoteCount.votes AS votes \
//         FROM Article LEFT JOIN VoteCount ON (Article.aid = VoteCount.aid) \
//         WHERE Article.aid = ?;");
//
// // get read handles
// let mut awvc = wp.view("ArticleWithVoteCount").unwrap();
// // looking up article 42 should yield the article we inserted with a vote count of 1
// assert_eq!(
//     awvc.lookup(&[aid.into()], true).unwrap(),
//     vec![vec![DataType::from(aid), title.into(), url.into(), 1.into()]]
// );
