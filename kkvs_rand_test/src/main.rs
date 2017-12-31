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

#![ feature( conservative_impl_trait ) ]

extern crate rand;
extern crate futures;
extern crate kkvs;

use std::fs::File;
use std::io::prelude::*;

use rand::{ Rng, thread_rng };
use rand::distributions::{ Sample, Range };

use futures::future::*;

use kkvs::Connection;

type Key = u16;
type Val = Vec< u8 >;
type Conn = Connection< Key, Val >;

struct Register {
    key: Key,
    val: Val,
    ops: usize
}

impl Register {

    fn then_self< T, E, F >( mut self, future: F ) -> impl Future< Item=Self, Error=() > where F: Future< Item=T, Error=E > {

        self.ops += 1;

        future.then( move | _ | ok( self ) )
    }

    fn set( self, conn: &Conn ) -> impl Future< Item=Self, Error=() > {

        let key = self.key;
        let val = self.val.clone();

        self.then_self( conn.set( key, val ) )
    }

    fn get( self, conn: &Conn ) -> impl Future< Item=Self, Error=() > {

        let key = self.key;

        self.then_self( conn.get( key ) )
    }

    fn del( self, conn: &Conn ) -> impl Future< Item=Self, Error=() > {

        let key = self.key;

        self.then_self( conn.del( key ) )
    }

    fn repeat( self ) -> impl Future< Item=Loop< Self, Self >, Error=() > {

        if self.ops < 90 {
            ok( Loop::Continue( self ) )
        } else {
            ok( Loop::Break( self ) )
        }
    }
}

fn rand_key< R: Rng >( rng: &mut R ) -> Key {

    rng.gen()
}

fn rand_val< R: Rng >( rng: &mut R, val_size: &mut Range< usize > ) -> Val {

    let val_size = val_size.sample( rng );

    let mut val = Vec::with_capacity( val_size );

    for _ in 0 .. val_size {
        val.push( rng.gen() );
    }

    val
}

fn rand_regs() -> Vec< Register > {

    let mut rng = thread_rng();
    let mut val_size = Range::new( 64, 512 );

    let mut regs: Vec< Register > = Vec::with_capacity( 4096 );

    for _ in 0 .. regs.capacity() {
        regs.push(
            Register {
                key: rand_key( &mut rng ),
                val: rand_val( &mut rng, &mut val_size ),
                ops: 0
            }
        );
    }

    regs
}

fn read_snapshot() -> Option< Vec< u8 > > {

    File::open( "snapshot.kkvs" ).ok().map(
        | mut file | {
            let mut buffer = Vec::new();
            file.read_to_end( &mut buffer ).unwrap();
            buffer
        }
    )
}

fn write_snapshot( snapshot: &[ u8 ] ) {

    File::create( "snapshot.kkvs" ).unwrap().write_all( &snapshot ).unwrap()
}

fn main() {

    let snapshot = read_snapshot();

    let conn = Connection::new(
        "localhost:9092".to_owned(),
        "kkvs_rand_test".to_owned(),
        snapshot.as_ref().map( Vec::as_slice )
    ).unwrap();

    let regs = rand_regs();

    join_all(
        regs.into_iter().map(
            | reg | {
                loop_fn(
                    reg,
                    | reg | {
                        reg.set( &conn )
                            .and_then( | reg | reg.get( &conn ) )
                            .and_then( | reg | reg.del( &conn ) )
                            .and_then( | reg | reg.repeat() )
                    }
                ).and_then( | reg | reg.set( &conn ) )
            }
        )
    ).wait().unwrap();

    write_snapshot( &conn.snapshot().wait().unwrap() );
}
