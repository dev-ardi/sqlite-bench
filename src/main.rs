use core::panic;
use human_bytes::human_bytes;
use rand::random;
use rusqlite::Connection;
use std::{fs, hint::black_box, time::Instant};

fn main() {
    _ = fs::remove_file("tmp.db");
    let con = rusqlite::Connection::open("tmp.db").unwrap();

    for i in 1..10usize {
        let size = i.pow(10);
        bench(&con, size);
    }
}

fn bench(con: &Connection, size: usize) {
    let buf: Vec<u8> = vec![random(); size];
    let iters = 1000;
    println!(
        "Benchmark for size {}, iters: {iters} * 2",
        human_bytes(size as f64)
    );

    con.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)", ())
        .unwrap();
    let t0 = Instant::now();
    let sql_idx = (0..iters)
        .map(|_| {
            con.execute("INSERT INTO blobs (data) VALUES (?1)", [&buf])
                .unwrap();
            con.last_insert_rowid()
        })
        .collect::<Vec<_>>();
    println!("sql_write: {:?}", t0.elapsed());

    let t0 = Instant::now();
    for idx in sql_idx {
        let buf: Vec<u8> = con
            .query_row("SELECT data FROM blobs WHERE id = ?1", [idx], |row| {
                row.get(0)
            })
            .unwrap();
        black_box(buf);
    }
    println!("sql_read: {:?}", t0.elapsed());

    con.execute("DROP TABLE blobs", ()).unwrap();

    fs::create_dir("tmp").unwrap();
    let t0 = Instant::now();
    for i in 0..iters {
        fs::write(i.to_string(), &buf).unwrap();
    }
    println!("fs_write: {:?}", t0.elapsed());

    // drop fs cache if we can
    if cfg!(unix) && std::process::id() == 0 {
        fs::write("/proc/sys/vm/drop_caches", "3").unwrap();
    }

    let t0 = Instant::now();
    for i in 0..iters {
        let buf = fs::read(i.to_string()).unwrap();
        black_box(buf);
    }
    println!("fs_read: {:?}", t0.elapsed());

    fs::remove_dir_all("tmp").unwrap();
}

