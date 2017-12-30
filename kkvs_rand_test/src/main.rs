#![ feature( conservative_impl_trait ) ]

extern crate rand;
extern crate futures;
extern crate kkvs;

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

    fn repeat( self ) -> impl Future< Item=Loop< (), Self >, Error=() > {

        if self.ops < 90 {
            ok( Loop::Continue( self ) )
        } else {
            ok( Loop::Break( () ) )
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

fn main() {

    let conn = Connection::new( "localhost:9092".to_owned(), "kkvs_rand_test".to_owned() ).unwrap();

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
                )
            }
        )
    ).wait().unwrap();
}
