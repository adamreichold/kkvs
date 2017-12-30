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

extern crate futures;
extern crate kafka;
extern crate serde;
#[ macro_use ]
extern crate serde_derive;
extern crate bincode;

use std::hash::Hash;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{ AtomicBool, Ordering };

use futures::future::*;
use futures::sync::oneshot;

use kafka::consumer::{ Consumer, Message, FetchOffset };
use kafka::producer::{ Producer, Record };

use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use bincode::{ serialize, deserialize, Infinite };


#[ derive( Debug, Clone, Copy, PartialEq ) ]
pub enum Error {
    Internal,
    Connect,
    Produce,
    Serialize,
    KeyNotFound,
    ConflictingWrite,
    IncompatibleSnapshot
}


type Completion = oneshot::Sender< Result< (), Error > >;

#[ derive( Debug ) ]
enum CompletionHolder {
    Zero,
    One( Completion ),
    Many( Vec< Completion > )
}

impl CompletionHolder {

    fn push( &mut self, new_sender: Completion ) {

        *self = match std::mem::replace( self, CompletionHolder::Zero ) {

            CompletionHolder::Zero => CompletionHolder::One( new_sender ),

            CompletionHolder::One( sender ) => CompletionHolder::Many( vec![ sender, new_sender ] ),

            CompletionHolder::Many( mut sender ) => { sender.push( new_sender ); CompletionHolder::Many( sender ) }

        };
    }

    fn call( self, result: Result< (), Error > ) {

        match self {

            CompletionHolder::Zero => {
            }

            CompletionHolder::One( sender ) => {
                let _ = sender.send( result );
            }

            CompletionHolder::Many( sender ) => {
                sender.into_iter().for_each( | sender | { let _ = sender.send( result ); } );
            }

        }
    }
}


#[ derive( Serialize, Deserialize ) ]
struct Update< V > {
    new_value: Option< V >,
    old_offset: i64
}

const OFFSET_NEW: i64 = -1;
const OFFSET_LWW: i64 = -2;

fn send_update< K, V >( prod: &mut Producer, topic: &String, key: &K, update: &Update< V > ) -> Result< (), Error > where K: Serialize, V: Serialize {

    let key = serialize( key, Infinite ).map_err( | _ | Error::Serialize )?;
    let update = serialize( update, Infinite ).map_err( | _ | Error::Serialize )?;

    prod.send( &Record::from_key_value( topic, key.as_slice(), update.as_slice() ) ).map_err( | _ | Error::Produce )
}

fn recv_update< K, V >( message: &Message ) -> ( K, Update< V >, i64 ) where K: DeserializeOwned, V: DeserializeOwned {

    let key: K = deserialize( message.key ).unwrap();
    let update: Update< V > = deserialize( message.value ).unwrap();

    ( key, update, message.offset )
}


type Values< K, V > = HashMap< K, ( V, i64 ) >;

fn update_value< K, V >( values: &mut Values< K, V >, key: K, new_value: Option< V >, old_offset: i64, new_offset: i64 ) -> bool where K: Eq + Hash {

    match values.entry( key ) {

        Entry::Vacant( entry ) => {

            if old_offset == OFFSET_LWW || ( old_offset == OFFSET_NEW && new_value.is_some() ) {
                entry.insert( ( new_value.unwrap(), new_offset ) );
                return true;
            }

        }

        Entry::Occupied( mut entry ) => {

            match new_value {

                None => {
                    let ( _, offset ) = *entry.get();

                    if old_offset == OFFSET_LWW || old_offset == offset {
                        entry.remove();
                        return true;
                    }
                }

                Some( new_value ) => {
                    let ( ref mut value, ref mut offset ) = *entry.get_mut();

                    if old_offset == OFFSET_LWW || old_offset == *offset {
                        *value = new_value;
                        *offset = new_offset;
                        return true;
                    }
                }

            }

        }

    }

    false
}


fn get< K, V >( values: &Values< K, V >, key: K, sender: oneshot::Sender< Option< V > > ) where K: Eq + Hash, V: Clone {

    let value = values.get( &key ).map( | &( ref value, _ ) | value.clone() );

    let _ = sender.send( value );
}

type PendingWrites< K, V > = HashMap< K, ( Option< V >, CompletionHolder ) >;

fn set< K, V >( prod: &mut Producer, topic: &String, values: &Values< K, V >, pending_writes: &mut PendingWrites< K, V >, key: K, value: Option< V >, lww: bool, sender: Completion ) where K: Eq + Hash + Serialize, V: PartialEq + Serialize {

    if lww {

        let update: Update< V > = Update{
            new_value: value,
            old_offset: OFFSET_LWW
        };

        let _ = sender.send( send_update( prod, topic, &key, &update ) );
        return;
    }

    let offset = values.get( &key ).map( | &( _, offset ) | offset );

    if value.is_none() && offset.is_none() {
        let _ = sender.send( Err( Error::KeyNotFound ) );
        return;
    }

    match pending_writes.entry( key ) {

        Entry::Occupied( mut entry ) => {

            if entry.get().0 != value {
                let _ = sender.send( Err( Error::ConflictingWrite ) );
                return;
            }

            entry.get_mut().1.push( sender );
        }

        Entry::Vacant( entry ) => {

            let update: Update< V > = Update{
                new_value: value,
                old_offset: offset.unwrap_or( OFFSET_NEW )
            };

            if let Err( err ) = send_update( prod, topic, entry.key(), &update ) {
                let _ = sender.send( Err( err ) );
                return;
            }

            entry.insert( ( update.new_value, CompletionHolder::One( sender ) ) );
        }

    };
}

fn receive< K, V >( values: &mut Values< K, V >, pending_writes: &mut PendingWrites< K, V >, key: K, new_value: Option< V >, old_offset: i64, new_offset: i64 ) where K: Clone + Eq + Hash, V: PartialEq {

    match pending_writes.entry( key ) {

        Entry::Vacant( entry ) => {
            update_value( values, entry.into_key(), new_value, old_offset, new_offset );
        }

        Entry::Occupied( entry ) => {

            let result = if entry.get().0 == new_value {
                Ok( () )
            } else {
                Err( Error::ConflictingWrite )
            };

            if update_value( values, entry.key().clone(), new_value, old_offset, new_offset ) {
                entry.remove().1.call( result );
            }
        }

    }
}

fn snapshot< K, V >( values: &Values< K, V >, sender: oneshot::Sender< Result< Vec< u8 >, Error > > ) where K: Eq + Hash + Serialize, V: Serialize {

    let _ = sender.send( serialize( values, Infinite ).map_err( | _ | Error::Serialize ) );
}


enum Command< K, V > {
    Get( K, oneshot::Sender< Option< V > > ),
    Set( K, Option< V >, bool, Completion ),
    Receive( K, Option< V >, i64, i64 ),
    Snapshot( oneshot::Sender< Result< Vec< u8 >, Error > > ),
    Disconnect
}

fn start_polling< K: 'static, V: 'static >( mut cons: Consumer, sender: &mpsc::Sender< Command< K, V > >, keep_polling: &Arc< AtomicBool > ) -> thread::JoinHandle< () > where K: Send + DeserializeOwned, V: Send + DeserializeOwned {

    let sender = sender.clone();

    let keep_polling = Arc::clone( keep_polling );

    thread::spawn(
        move || {
            while keep_polling.load( Ordering::Relaxed ) {

                for message_set in cons.poll().unwrap().iter() {
                    for message in message_set.messages() {

                        let ( key, update, new_offset ) = recv_update( message );

                        sender.send( Command::Receive( key, update.new_value, update.old_offset, new_offset ) ).unwrap();
                    }
                }
            }
        }
    )
}

fn start< K: 'static, V: 'static >( mut prod: Producer, topic: String, receiver: mpsc::Receiver< Command< K, V > >, keep_polling: Arc< AtomicBool >, poll_handle: thread::JoinHandle< () >, mut values: Values< K, V > ) -> thread::JoinHandle< () > where K: Send + Clone + Eq + Hash + Serialize, V: Send + Clone + PartialEq + Serialize {

    thread::spawn(
        move || {

            let mut pending_writes = PendingWrites::new();

            loop {
                match receiver.recv().unwrap() {

                    Command::Get( key, sender ) => get( &values, key, sender ),

                    Command::Set( key, value, lww, sender ) => set( &mut prod, &topic, &values, &mut pending_writes, key, value, lww, sender ),

                    Command::Receive( key, new_value, old_offset, new_offset ) => receive( &mut values, &mut pending_writes, key, new_value, old_offset, new_offset ),

                    Command::Snapshot( sender ) => snapshot( &values, sender ),

                    Command::Disconnect => break

                }
            }

            keep_polling.store( false, Ordering::Release );
            let _ = poll_handle.join();
        }
    )
}

struct Worker< K, V > {
    handle: Option< thread::JoinHandle< () > >,
    sender: mpsc::Sender< Command< K, V > >
}

impl< K: 'static, V: 'static > Worker< K, V > where K: Send + Clone + Eq + Hash + Serialize + DeserializeOwned, V: Send + Clone + PartialEq + Serialize + DeserializeOwned {

    fn new( host: String, topic: String, snapshot: Option< &[ u8 ] > ) -> Result< Self, Error > {

        let values = if let Some( snapshot ) = snapshot {
            deserialize( snapshot ).map_err( | _ | Error::IncompatibleSnapshot )?
        } else {
            Values::new()
        };

        let cons = Consumer::from_hosts( vec!( host.clone() ) ).with_topic( topic.clone() ).with_fallback_offset( FetchOffset::Earliest ).create().map_err( | _ | Error::Connect )?;
        let prod = Producer::from_hosts( vec!( host ) ).create().map_err( | _ | Error::Connect )?;

        let ( sender, receiver ) = mpsc::channel();

        let keep_polling = Arc::new( AtomicBool::new( true ) );

        let poll_handle = start_polling( cons, &sender, &keep_polling );

        let handle = start( prod, topic, receiver, keep_polling, poll_handle, values );

        Ok(
            Self {
                handle: Some( handle ),
                sender: sender
            }
        )
    }
}

impl< K, V > Drop for Worker< K, V > {

    fn drop( &mut self ) {

        self.sender.send( Command::Disconnect ).unwrap();
        let _ = self.handle.take().unwrap().join();
    }
}


#[ derive( Clone ) ]
pub struct Connection< K, V > {
    worker: Arc< Mutex< Worker< K, V > > >,
    sender: mpsc::Sender< Command< K, V > >
}

impl< K: 'static, V: 'static > Connection< K, V > where K: Send + Clone + Eq + Hash + Serialize + DeserializeOwned, V: Send + Clone + PartialEq + Serialize + DeserializeOwned {

    pub fn new( host: String, topic: String, snapshot: Option< &[ u8 ] > ) -> Result< Self, Error > {

        let worker = Worker::new( host, topic, snapshot )?;

        let sender = worker.sender.clone();

        Ok(
            Self {
                worker: Arc::new( Mutex::new( worker ) ),
                sender: sender
            }
        )
    }

    pub fn get( &self, key: K ) -> impl Future< Item=Option< V >, Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Get( key, sender ), receiver )
    }

    pub fn set( &self, key: K, value: V ) -> impl Future< Item=(), Error=Error > {

        self.send_set( key, Some( value ), false )
    }

    pub fn set_lww( &self, key: K, value: V) -> impl Future< Item=(), Error=Error > {

        self.send_set( key, Some( value ), true )
    }

    pub fn del( &self, key: K ) -> impl Future< Item=(), Error=Error > {

        self.send_set( key, None, false )
    }

    pub fn del_lww( &self, key: K ) -> impl Future< Item=(), Error=Error > {

        self.send_set( key, None, true )
    }

    pub fn snapshot( &self ) -> impl Future< Item=Vec< u8 >, Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Snapshot( sender ), receiver ).and_then( result )
    }

    fn send_set( &self, key: K, value: Option< V >, lww: bool ) -> impl Future< Item=(), Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Set( key, value, lww, sender ), receiver ).and_then( result )
    }

    fn send_cmd< T >( &self, cmd: Command< K, V >, receiver: oneshot::Receiver< T > ) -> impl Future< Item=T, Error=Error > {

        let send_result = self.sender.send( cmd ).map_err( | _ | { Error::Internal } );

        result( send_result ).and_then(
            move | _ | receiver.map_err( | _ | Error::Internal )
        )
    }
}


#[ cfg( test ) ]
mod tests {

    extern crate rand;

    use super::*;

    fn sleep( millis: u64 ) {

        use std::thread::sleep;
        use std::time::Duration;

        sleep( Duration::from_millis( millis ) );
    }

    fn rand_sleep( millis: u64 ) {

        use self::rand::{ Rng, thread_rng };

        sleep( thread_rng().gen_range( 0, millis ) );
    }

    fn rand_str() -> String {

        use self::rand::{ Rng, thread_rng };

        thread_rng().gen_ascii_chars().take( 64 ).collect()
    }

    fn sync_one( conn: &Connection< String, String > ) -> ( String, String ) {

        let key = rand_str();
        let value = rand_str();

        assert_eq!( conn.set( key.clone(), value.clone() ).wait(), Ok( () ) );

        ( key, value )
    }

    fn sync_two( first_conn: &Connection< String, String >, second_conn: &Connection< String, String > ) -> ( String, String ) {

        let ( key, value ) = sync_one( first_conn );

        while second_conn.get( key.clone() ).wait().unwrap().as_ref() != Some( &value ) {
            sleep( 10 );
        }

        ( key, value )
    }

    fn connect() -> Connection< String, String > {

        Connection::new( "localhost:9092".to_owned(), "kkvs_test".to_owned(), None ).unwrap()
    }

    fn connect_and_sync() -> Connection< String, String > {

        let conn = connect();

        sync_one( &conn );

        conn
    }

    #[ test ]
    fn can_connect() {

        connect();
    }

    #[ test ]
    fn can_set_get_del() {

        let conn = connect_and_sync();

        let key = "foo".to_owned();
        let value = "bar".to_owned();

        assert_eq!( conn.set( key.clone(), value.clone() ).wait(), Ok( () ) );

        assert_eq!( conn.get( key.clone() ).wait(), Ok( Some( value.clone() ) ) );

        assert_eq!( conn.del( key.clone() ).wait(), Ok( () ) );
    }

    #[ test ]
    fn can_set() {

        let conn = connect_and_sync();

        let key = "foobar".to_owned();
        let first_value = "foo".to_owned();
        let second_value = "bar".to_owned();

        assert_eq!( conn.set( key.clone(), first_value.clone() ).wait(), Ok( () ) );

        assert_eq!( conn.get( key.clone() ).wait(), Ok( Some( first_value.clone() ) ) );

        assert_eq!( conn.set( key.clone(), second_value.clone() ).wait(), Ok( () ) );

        assert_eq!( conn.get( key.clone() ).wait(), Ok( Some( second_value.clone() ) ) );
    }

    #[ test ]
    fn can_del() {

        let conn = connect_and_sync();

        let key = "bar".to_owned();
        let value = "foo".to_owned();

        assert_eq!( conn.set( key.clone(), value.clone() ).wait(), Ok( () ) );

        assert_eq!( conn.get( key.clone() ).wait(), Ok( Some( value.clone() ) ) );

        assert_eq!( conn.del( key.clone() ).wait(), Ok( () ) );

        assert_eq!( conn.get( key.clone() ).wait(), Ok( None ) );
    }

    #[ test ]
    fn cannot_del() {

        let conn = connect_and_sync();

        let key = "barfoo".to_owned();

        assert_eq!( conn.get( key.clone() ).wait(), Ok( None ) );

        assert_eq!( conn.del( key.clone() ).wait(), Err( Error::KeyNotFound ) );
    }

    fn perform_racing_writes( key: &String, initial_value: &String, first_value: &String, second_value: &String, clone: bool, lww: bool ) -> ( String, Result< (), Error >, Result< (), Error > ) {

        let first_conn = connect();

        let second_op = {
            let second_conn = if clone {
                first_conn.clone()
            } else {
                connect()
            };

            sync_two( &first_conn, &second_conn );

            assert_eq!( first_conn.set( key.clone(), initial_value.clone() ).wait(), Ok( () ) );

            sync_two( &first_conn, &second_conn );

            let key = key.clone();
            let second_value = second_value.clone();

            thread::spawn(
                move || {

                    rand_sleep( 100 );

                    let second_op = if lww {
                        second_conn.set_lww( key, second_value ).wait()
                    } else {
                        second_conn.set( key, second_value ).wait()
                    };

                    second_op
                }
            )
        };

        rand_sleep( 100 );

        let first_op = if lww {
            first_conn.set_lww( key.clone(), first_value.clone() ).wait()
        } else {
            first_conn.set( key.clone(), first_value.clone() ).wait()
        };

        let second_op = second_op.join().unwrap();

        sync_one( &first_conn );

        let final_value = first_conn.get( key.clone() ).wait().unwrap().unwrap();

        ( final_value, first_op, second_op )
    }

    #[ test ]
    fn can_detect_conflicting_writes() {

        for &clone in [ true, false ].iter() {

            let key = "some_key".to_owned();
            let initial_value = "initial_value".to_owned();
            let first_value = "some_value".to_owned();
            let second_value = "some_other_value".to_owned();

            let ( final_value, first_op, second_op ) = perform_racing_writes( &key, &initial_value, &first_value, &second_value, clone, false );

            assert!( first_op.is_ok() || second_op.is_ok() );

            if first_op.is_ok() && second_op.is_ok() {
                assert!( final_value == first_value || final_value == second_value );
            } else if first_op.is_ok() && second_op.is_err() {
                assert_eq!( final_value, first_value );
                assert_eq!( second_op.err().unwrap(), Error::ConflictingWrite );
            } else if first_op.is_err() && second_op.is_ok() {
                assert_eq!( first_op.err().unwrap(), Error::ConflictingWrite );
                assert_eq!( final_value, second_value );
            }
        }
    }

    #[ test ]
    fn can_use_last_write_wins() {

        for &clone in [ true, false ].iter() {

            let key = "some_other_key".to_owned();
            let initial_value = "initial_value".to_owned();
            let first_value = "some_value".to_owned();
            let second_value = "some_other_value".to_owned();

            let ( final_value, first_op, second_op ) = perform_racing_writes( &key, &initial_value, &first_value, &second_value, clone, true );

            assert_eq!( first_op, Ok( () ) );
            assert_eq!( second_op, Ok( () ) );
            assert!( final_value == first_value || final_value == second_value );
        }
    }
}
