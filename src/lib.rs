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
use std::sync::atomic::{ AtomicBool, Ordering };

use futures::future::*;
use futures::sync::oneshot;

use kafka::consumer::{ Consumer, Message };
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
    ConflictingWrite
}


type Completion = oneshot::Sender< Result< (), Error > >;

enum CompletionHolder {
    Zero,
    One( Completion ),
    Many( Vec< Completion > )
}

impl CompletionHolder {

    fn push( &mut self, new_sender: Completion ) {

        let new_self = match std::mem::replace( self, CompletionHolder::Zero ) {

            CompletionHolder::Zero => CompletionHolder::One( new_sender ),

            CompletionHolder::One( sender ) => CompletionHolder::Many( vec![ sender, new_sender ] ),

            CompletionHolder::Many( mut sender ) => { sender.push( new_sender ); CompletionHolder::Many( sender ) }

        };

        *self = new_self;
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


type PendingWrites< K, V > = HashMap< ( K, i64 ), ( Option< V >, CompletionHolder ) >;

fn remove_pending_write< K, V >( pending_writes: &mut PendingWrites< K, V >, key: K, new_value: &Option< V >, old_offset: i64 ) -> ( K, Option< ( bool, CompletionHolder ) > ) where K: Eq + Hash, V: PartialEq {

    match pending_writes.entry( ( key, old_offset ) ) {

        Entry::Vacant( entry ) => ( entry.into_key().0, None ),

        Entry::Occupied( entry ) => {

            let ( ( key, _ ), ( expected_value, holder ) ) = entry.remove_entry();

            ( key, Some( ( expected_value != *new_value, holder ) ) )
        }
    }
}


type Values< K, V > = HashMap< K, ( V, i64 ) >;

fn update_value< K, V >( values: &mut Values< K, V >, key: K, new_value: Option< V >, old_offset: i64, new_offset: i64 ) -> bool where K: Eq + Hash {

    match values.entry( key ) {

        Entry::Vacant( entry ) => {

            if new_value.is_some() {
                entry.insert( ( new_value.unwrap(), new_offset ) );

                true
            } else {
                false
            }

        }

        Entry::Occupied( mut entry ) => {

            match new_value {

                None => {
                    let ( _, offset ) = *entry.get();

                    if offset == old_offset {
                        entry.remove();

                        true
                    } else {
                        false
                    }
                }

                Some( new_value ) => {
                    let ( ref mut value, ref mut offset ) = *entry.get_mut();

                    if *offset == old_offset {
                        *value = new_value;
                        *offset = new_offset;

                        true
                    } else {
                        false
                    }
                }

            }

        }

    }
}


fn get< K, V >( values: &Values< K, V >, key: K, sender: oneshot::Sender< Option< V > > ) where K: Eq + Hash, V: Clone {

    let value = values.get( &key ).map( | &( ref value, _ ) | value.clone() );

    let _ = sender.send( value );
}

fn set< K, V >( prod: &mut Producer, topic: &String, values: &Values< K, V >, pending_writes: &mut PendingWrites< K, V >, key: K, value: Option< V >, sender: Completion ) where K: Eq + Hash + Serialize, V: PartialEq + Serialize {

    let offset = values.get( &key ).map( | &( _, offset ) | offset );

    if value.is_none() && offset.is_none() {
        let _ = sender.send( Err( Error::KeyNotFound ) );
        return;
    }

    let update: Update< V > = Update{
        new_value: value,
        old_offset: offset.unwrap_or( -1 )
    };

    match pending_writes.entry( ( key, update.old_offset ) ) {

        Entry::Occupied( mut entry ) => {

            if update.new_value != entry.get().0 {
                let _ = sender.send( Err( Error::ConflictingWrite ) );
                return;
            }

            entry.get_mut().1.push( sender );
        }

        Entry::Vacant( entry ) => {

            if let Err( err ) = send_update( prod, topic, entry.key(), &update ) {
                let _ = sender.send( Err( err ) );
                return;
            }

            entry.insert( ( update.new_value, CompletionHolder::One( sender ) ) );
        }

    };
}

fn receive< K, V >( values: &mut Values< K, V >, pending_writes: &mut PendingWrites< K, V >, key: K, new_value: Option< V >, old_offset: i64, new_offset: i64 ) where K: Eq + Hash, V: PartialEq {

    let ( key, pending_write ) = remove_pending_write( pending_writes, key, &new_value, old_offset );

    if update_value( values, key, new_value, old_offset, new_offset ) {

        if let Some( ( conflict, holder ) ) = pending_write {
            let _ = holder.call(
                if conflict {
                    Err( Error::ConflictingWrite )
                } else {
                    Ok( () )
                }
            );
        }

    }
}


enum Command< K, V > {
    Get( K, oneshot::Sender< Option< V > > ),
    Set( K, Option< V >, Completion ),
    Receive( K, Option< V >, i64, i64 ),
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

fn start< K: 'static, V: 'static >( mut prod: Producer, topic: String, receiver: mpsc::Receiver< Command< K, V > >, keep_polling: Arc< AtomicBool >, poll_handle: thread::JoinHandle< () > ) -> thread::JoinHandle< () > where K: Send + Eq + Hash + Serialize, V: Send + Clone + PartialEq + Serialize {

    thread::spawn(
        move || {

            let mut values = Values::new();

            let mut pending_writes = PendingWrites::new();

            loop {
                match receiver.recv().unwrap() {

                    Command::Get( key, sender ) => get( &values, key, sender ),

                    Command::Set( key, value, sender ) => set( &mut prod, &topic, &values, &mut pending_writes, key, value, sender ),

                    Command::Receive( key, new_value, old_offset, new_offset ) => receive( &mut values, &mut pending_writes, key, new_value, old_offset, new_offset ),

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

impl< K: 'static, V: 'static > Worker< K, V > where K: Send + Eq + Hash + Serialize + DeserializeOwned, V: Clone + Send + PartialEq + Serialize + DeserializeOwned {

    fn new( host: String, topic: String ) -> Result< Self, Error > {

        let cons = Consumer::from_hosts( vec!( host.clone() ) ).with_topic( topic.clone() ).create().map_err( | _ | Error::Connect )?;
        let prod = Producer::from_hosts( vec!( host ) ).create().map_err( | _ | Error::Connect )?;

        let ( sender, receiver ) = mpsc::channel();

        let keep_polling = Arc::new( AtomicBool::new( true ) );

        let poll_handle = start_polling( cons, &sender, &keep_polling );

        let handle = start( prod, topic, receiver, keep_polling, poll_handle );

        Ok(
            Self {
                handle: Some( handle ),
                sender: sender
            }
        )
    }
}

impl< K, V > Drop for Worker< K, V > {

    fn drop(&mut self) {

        self.sender.send( Command::Disconnect ).unwrap();
        let _ = self.handle.take().unwrap().join();
    }
}


#[ derive( Clone ) ]
pub struct Connection< K, V > {
    worker: Arc< Worker< K, V > >,
    sender: mpsc::Sender< Command< K, V > >
}

impl< K: 'static, V: 'static > Connection< K, V > where K: Send + Eq + Hash + Serialize + DeserializeOwned, V: Clone + Send + PartialEq + Serialize + DeserializeOwned {

    pub fn new( host: String, topic: String ) -> Result< Self, Error > {

        let worker = Worker::new( host, topic )?;

        let sender = worker.sender.clone();

        Ok(
            Self {
                worker: Arc::new( worker ),
                sender: sender
            }
        )
    }

    pub fn get( &self, key: K ) -> impl Future< Item=Option< V >, Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Get( key, sender ), receiver )
    }

    pub fn get_sync( &self, key: K ) -> Result< Option< V >, Error > {

        self.get( key ).wait()
    }

    pub fn set( &self, key: K, value: V ) -> impl Future< Item=(), Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Set( key, Some( value ), sender ), receiver ).and_then( result )
    }

    pub fn set_sync( &self, key: K, value: V) -> Result< (), Error > {

        self.set( key, value ).wait()
    }

    pub fn del( &self, key: K ) -> impl Future< Item=(), Error=Error > {

        let ( sender, receiver ) = oneshot::channel();

        self.send_cmd( Command::Set( key, None, sender ), receiver ).and_then( result )
    }

    pub fn del_sync( &self, key: K ) -> Result< (), Error > {

        self.del( key ).wait()
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

    fn connect_to_test() -> Connection< String, String > {

        Connection::new( "localhost:9092".to_owned(), "kkvs_test".to_owned() ).unwrap()
    }

    fn rand_sleep() {

        use std::thread::sleep;
        use std::time::Duration;
        use self::rand::{ Rng, thread_rng };

        sleep( Duration::from_millis( thread_rng().gen_range( 0, 500 ) ) );
    }

    #[ test ]
    fn can_connect() {

        connect_to_test();
    }

    #[ test ]
    fn can_set_get_del() {

        let conn = connect_to_test();

        let key = "foo".to_owned();
        let value = "bar".to_owned();

        assert_eq!( conn.set_sync( key.clone(), value.clone() ), Ok( () ) );

        assert_eq!( conn.get_sync( key.clone() ), Ok( Some( value.clone() ) ) );

        assert_eq!( conn.del_sync( key.clone() ), Ok( () ) );
    }

    #[ test ]
    fn can_set() {

        let conn = connect_to_test();

        let key = "foobar".to_owned();
        let first_value = "foo".to_owned();
        let second_value = "bar".to_owned();

        assert_eq!( conn.set_sync( key.clone(), first_value.clone() ), Ok( () ) );

        assert_eq!( conn.get_sync( key.clone() ), Ok( Some( first_value.clone() ) ) );

        assert_eq!( conn.set_sync( key.clone(), second_value.clone() ), Ok( () ) );

        assert_eq!( conn.get_sync( key.clone() ), Ok( Some( second_value.clone() ) ) );
    }

    #[ test ]
    fn can_del() {

        let conn = connect_to_test();

        let key = "bar".to_owned();
        let value = "foo".to_owned();

        assert_eq!( conn.set_sync( key.clone(), value.clone() ), Ok( () ) );

        assert_eq!( conn.get_sync( key.clone() ), Ok( Some( value.clone() ) ) );

        assert_eq!( conn.del_sync( key.clone() ), Ok( () ) );

        assert_eq!( conn.get_sync( key.clone() ), Ok( None ) );
    }

    #[ test ]
    fn cannot_del() {

        let conn = connect_to_test();

        let key = "barfoo".to_owned();

        assert_eq!( conn.get_sync( key.clone() ), Ok( None ) );

        assert_eq!( conn.del_sync( key.clone() ), Err( Error::KeyNotFound ) );
    }

    #[ test ]
    fn can_detect_conflict() {

        let conn = connect_to_test();

        let key = "some_key".to_owned();
        let initial_value = "initial_value".to_owned();
        let first_value = "some_value".to_owned();
        let second_value = "some_value".to_owned();
        let third_value = "some_other_value".to_owned();

        assert_eq!( conn.set_sync( key.clone(), initial_value.clone() ), Ok( () ) );

        let first_op = conn.set( key.clone(), first_value.clone() );

        rand_sleep();

        let second_op = conn.set( key.clone(), second_value.clone() );

        rand_sleep();

        let third_op = conn.set( key.clone(), third_value.clone() );

        assert_eq!( first_op.wait(), Ok( () ) );
        assert_eq!( second_op.wait(), Ok( () ) );

        match third_op.wait() {

            Ok( () ) => {
                assert_eq!( conn.get_sync( key.clone() ), Ok( Some( third_value.clone() ) ) );
            }

            Err( err ) => {
                assert_eq!( err, Error::ConflictingWrite );

                assert_eq!( conn.get_sync( key.clone() ), Ok( Some( second_value.clone() ) ) );
            }

        }
    }
}
