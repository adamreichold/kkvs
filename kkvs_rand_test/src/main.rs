/*

Copyright 2017 Adam Reichod

This file is part of kkvs.

kkvs is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

kkvs is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with kkvs.  If not, see <http://www.gnu.org/licenses/>.

*/
use std::fs::File;
use std::io::prelude::*;

use futures::{executor::block_on, future::ready, prelude::*, stream::FuturesUnordered};
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};

use kkvs::Connection;

type Key = u16;
type Val = Vec<u8>;

struct Register {
    key: Key,
    val: Val,
}

fn rand_key<R: Rng>(rng: &mut R) -> Key {
    rng.gen()
}

fn rand_val<R: Rng>(rng: &mut R, val_size: &mut Uniform<usize>) -> Val {
    let val_size = val_size.sample(rng);

    let mut val = Vec::with_capacity(val_size);

    rng.fill(val.as_mut_slice());

    val
}

fn rand_regs() -> Vec<Register> {
    let mut rng = thread_rng();
    let mut val_size = Uniform::new(64, 512);

    let mut regs: Vec<Register> = Vec::with_capacity(4096);

    for _ in 0..regs.capacity() {
        regs.push(Register {
            key: rand_key(&mut rng),
            val: rand_val(&mut rng, &mut val_size),
        });
    }

    regs
}

fn read_snapshot() -> Option<Vec<u8>> {
    File::open("snapshot.kkvs").ok().map(|mut file| {
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        buffer
    })
}

fn write_snapshot(snapshot: &[u8]) {
    File::create("snapshot.kkvs")
        .unwrap()
        .write_all(&snapshot)
        .unwrap()
}

fn main() {
    let snapshot = read_snapshot();

    let conn = Connection::new(
        "localhost:9092".to_owned(),
        "kkvs_rand_test".to_owned(),
        snapshot.as_ref().map(Vec::as_slice),
    )
    .unwrap();

    let regs = rand_regs();

    let snapshot = block_on(async move {
        let conn = &conn;

        regs.into_iter()
            .map(|reg| {
                async move {
                    for _ops in 0..90 {
                        conn.set(reg.key, reg.val.clone()).await.unwrap();
                        conn.get(reg.key).await.unwrap();
                        conn.del(reg.key).await.unwrap();
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .for_each(|()| ready(()))
            .await;

        conn.snapshot().await.unwrap()
    });

    write_snapshot(&snapshot);
}
