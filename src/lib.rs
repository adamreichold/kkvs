/*

Copyright 2017, 2019 Adam Reichod

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

use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread::{spawn, JoinHandle};

use bincode::{deserialize, serialize};
use futures::{channel::oneshot, prelude::*};
use kafka::{
    consumer::{Consumer, FetchOffset, Message},
    producer::{Producer, Record},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Error {
    Internal,
    Connect,
    Produce,
    Serialize,
    Deserialize,
    KeyNotFound,
    ConflictingWrite,
    IncompatibleSnapshot,
}

type Completion = oneshot::Sender<Result<(), Error>>;

#[derive(Debug)]
enum CompletionHolder {
    Zero,
    One(Completion),
    Many(Vec<Completion>),
}

impl CompletionHolder {
    fn push(&mut self, new_sender: Completion) {
        *self = match std::mem::replace(self, CompletionHolder::Zero) {
            CompletionHolder::Zero => CompletionHolder::One(new_sender),

            CompletionHolder::One(sender) => CompletionHolder::Many(vec![sender, new_sender]),

            CompletionHolder::Many(mut sender) => {
                sender.push(new_sender);
                CompletionHolder::Many(sender)
            }
        };
    }

    fn call(self, result: Result<(), Error>) {
        match self {
            CompletionHolder::Zero => {}

            CompletionHolder::One(sender) => {
                let _ = sender.send(result);
            }

            CompletionHolder::Many(sender) => {
                sender.into_iter().for_each(|sender| {
                    let _ = sender.send(result);
                });
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Update<V> {
    new_value: Option<V>,
    old_offset: i64,
}

const OFFSET_NEW: i64 = -1;
const OFFSET_LWW: i64 = -2;

fn send_update<K, V>(
    prod: &mut Producer,
    topic: &str,
    key: &K,
    update: &Update<V>,
) -> Result<(), Error>
where
    K: Serialize,
    V: Serialize,
{
    let key = serialize(key).map_err(|_| Error::Serialize)?;
    let update = serialize(update).map_err(|_| Error::Serialize)?;

    prod.send(&Record::from_key_value(
        topic,
        key.as_slice(),
        update.as_slice(),
    ))
    .map_err(|_| Error::Produce)
}

fn recv_update<K, V>(message: &Message) -> Result<(K, Update<V>, i64), Error>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    let key: K = deserialize(message.key).map_err(|_| Error::Deserialize)?;
    let update: Update<V> = deserialize(message.value).map_err(|_| Error::Deserialize)?;

    Ok((key, update, message.offset))
}

type Values<K, V> = HashMap<K, (V, i64)>;

fn update_value<K, V>(
    values: &mut Values<K, V>,
    key: K,
    new_value: Option<V>,
    old_offset: i64,
    new_offset: i64,
) -> bool
where
    K: Eq + Hash,
{
    match values.entry(key) {
        Entry::Vacant(entry) => {
            if old_offset == OFFSET_LWW || (old_offset == OFFSET_NEW && new_value.is_some()) {
                entry.insert((new_value.unwrap(), new_offset));
                return true;
            }
        }

        Entry::Occupied(mut entry) => match new_value {
            None => {
                let (_, offset) = *entry.get();

                if old_offset == OFFSET_LWW || old_offset == offset {
                    entry.remove();
                    return true;
                }
            }

            Some(new_value) => {
                let (ref mut value, ref mut offset) = *entry.get_mut();

                if old_offset == OFFSET_LWW || old_offset == *offset {
                    *value = new_value;
                    *offset = new_offset;
                    return true;
                }
            }
        },
    }

    false
}

fn get<K, V>(values: &Values<K, V>, key: K, sender: oneshot::Sender<Option<V>>)
where
    K: Eq + Hash,
    V: Clone,
{
    let value = values.get(&key).map(|&(ref value, _)| value.clone());

    let _ = sender.send(value);
}

type PendingWrites<K, V> = HashMap<K, (Option<V>, CompletionHolder)>;

#[allow(clippy::too_many_arguments)]
fn set<K, V>(
    prod: &mut Producer,
    topic: &str,
    values: &Values<K, V>,
    pending_writes: &mut PendingWrites<K, V>,
    key: K,
    value: Option<V>,
    lww: bool,
    sender: Completion,
) where
    K: Eq + Hash + Serialize,
    V: PartialEq + Serialize,
{
    if lww {
        let update: Update<V> = Update {
            new_value: value,
            old_offset: OFFSET_LWW,
        };

        let _ = sender.send(send_update(prod, topic, &key, &update));
        return;
    }

    let offset = values.get(&key).map(|&(_, offset)| offset);

    if value.is_none() && offset.is_none() {
        let _ = sender.send(Err(Error::KeyNotFound));
        return;
    }

    match pending_writes.entry(key) {
        Entry::Occupied(mut entry) => {
            if entry.get().0 != value {
                let _ = sender.send(Err(Error::ConflictingWrite));
                return;
            }

            entry.get_mut().1.push(sender);
        }

        Entry::Vacant(entry) => {
            let update: Update<V> = Update {
                new_value: value,
                old_offset: offset.unwrap_or(OFFSET_NEW),
            };

            if let Err(err) = send_update(prod, topic, entry.key(), &update) {
                let _ = sender.send(Err(err));
                return;
            }

            entry.insert((update.new_value, CompletionHolder::One(sender)));
        }
    };
}

fn receive<K, V>(
    values: &mut Values<K, V>,
    pending_writes: &mut PendingWrites<K, V>,
    key: K,
    new_value: Option<V>,
    old_offset: i64,
    new_offset: i64,
) where
    K: Clone + Eq + Hash,
    V: PartialEq,
{
    match pending_writes.entry(key) {
        Entry::Vacant(entry) => {
            update_value(values, entry.into_key(), new_value, old_offset, new_offset);
        }

        Entry::Occupied(entry) => {
            let result = if entry.get().0 == new_value {
                Ok(())
            } else {
                Err(Error::ConflictingWrite)
            };

            if update_value(
                values,
                entry.key().clone(),
                new_value,
                old_offset,
                new_offset,
            ) {
                entry.remove().1.call(result);
            }
        }
    }
}

fn snapshot<K, V>(values: &Values<K, V>, sender: oneshot::Sender<Result<Vec<u8>, Error>>)
where
    K: Eq + Hash + Serialize,
    V: Serialize,
{
    let _ = sender.send(serialize(values).map_err(|_| Error::Serialize));
}

enum Command<K, V> {
    Get(K, oneshot::Sender<Option<V>>),
    Set(K, Option<V>, bool, Completion),
    Receive(K, Option<V>, i64, i64),
    Snapshot(oneshot::Sender<Result<Vec<u8>, Error>>),
    Disconnect,
}

fn start_polling<K, V>(
    mut cons: Consumer,
    sender: mpsc::Sender<Command<K, V>>,
    keep_polling: Arc<AtomicBool>,
) -> JoinHandle<()>
where
    K: 'static + Send + DeserializeOwned,
    V: 'static + Send + DeserializeOwned,
{
    spawn(move || {
        while keep_polling.load(Ordering::Relaxed) {
            for message_set in cons.poll().unwrap().iter() {
                for message in message_set.messages() {
                    if let Ok((key, update, new_offset)) = recv_update(message) {
                        sender
                            .send(Command::Receive(
                                key,
                                update.new_value,
                                update.old_offset,
                                new_offset,
                            ))
                            .unwrap();
                    }
                }
            }
        }
    })
}

fn start<K, V>(
    mut prod: Producer,
    topic: String,
    receiver: mpsc::Receiver<Command<K, V>>,
    keep_polling: Arc<AtomicBool>,
    poll_handle: JoinHandle<()>,
    mut values: Values<K, V>,
) -> JoinHandle<()>
where
    K: 'static + Send + Clone + Eq + Hash + Serialize,
    V: 'static + Send + Clone + PartialEq + Serialize,
{
    spawn(move || {
        let mut pending_writes = PendingWrites::new();

        loop {
            match receiver.recv().unwrap() {
                Command::Get(key, sender) => get(&values, key, sender),

                Command::Set(key, value, lww, sender) => set(
                    &mut prod,
                    &topic,
                    &values,
                    &mut pending_writes,
                    key,
                    value,
                    lww,
                    sender,
                ),

                Command::Receive(key, new_value, old_offset, new_offset) => receive(
                    &mut values,
                    &mut pending_writes,
                    key,
                    new_value,
                    old_offset,
                    new_offset,
                ),

                Command::Snapshot(sender) => snapshot(&values, sender),

                Command::Disconnect => break,
            }
        }

        keep_polling.store(false, Ordering::Release);
        let _ = poll_handle.join();
    })
}

struct Worker<K, V> {
    handle: Option<JoinHandle<()>>,
    sender: mpsc::Sender<Command<K, V>>,
}

impl<K: 'static, V: 'static> Worker<K, V>
where
    K: Send + Clone + Eq + Hash + Serialize + DeserializeOwned,
    V: Send + Clone + PartialEq + Serialize + DeserializeOwned,
{
    fn new(host: String, topic: String, values: Values<K, V>) -> Result<Self, Error> {
        let cons = Consumer::from_hosts(vec![host.clone()])
            .with_topic(topic.clone())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .map_err(|_| Error::Connect)?;
        let prod = Producer::from_hosts(vec![host])
            .create()
            .map_err(|_| Error::Connect)?;

        let (sender, receiver) = mpsc::channel();

        let keep_polling = Arc::new(AtomicBool::new(true));

        let poll_handle = start_polling(cons, sender.clone(), keep_polling.clone());

        let handle = start(prod, topic, receiver, keep_polling, poll_handle, values);

        Ok(Self {
            handle: Some(handle),
            sender,
        })
    }
}

impl<K, V> Drop for Worker<K, V> {
    fn drop(&mut self) {
        self.sender.send(Command::Disconnect).unwrap();
        let _ = self.handle.take().unwrap().join();
    }
}

#[derive(Clone)]
pub struct Connection<K, V> {
    worker: Arc<Mutex<Worker<K, V>>>,
    sender: mpsc::Sender<Command<K, V>>,
}

impl<K: 'static, V: 'static> Connection<K, V>
where
    K: Send + Clone + Eq + Hash + Serialize + DeserializeOwned,
    V: Send + Clone + PartialEq + Serialize + DeserializeOwned,
{
    pub fn new(host: String, topic: String, snapshot: Option<&[u8]>) -> Result<Self, Error> {
        let values = if let Some(snapshot) = snapshot {
            deserialize(snapshot).map_err(|_| Error::IncompatibleSnapshot)?
        } else {
            Values::new()
        };

        let worker = Worker::new(host, topic, values)?;

        let sender = worker.sender.clone();

        Ok(Self {
            worker: Arc::new(Mutex::new(worker)),
            sender,
        })
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, Error> {
        self.send_cmd(|sender| Command::Get(key, sender)).await
    }

    pub async fn set(&self, key: K, value: V) -> Result<(), Error> {
        self.send_set(key, Some(value), false).await
    }

    pub async fn set_lww(&self, key: K, value: V) -> Result<(), Error> {
        self.send_set(key, Some(value), true).await
    }

    pub async fn del(&self, key: K) -> Result<(), Error> {
        self.send_set(key, None, false).await
    }

    pub async fn del_lww(&self, key: K) -> Result<(), Error> {
        self.send_set(key, None, true).await
    }

    pub async fn snapshot(&self) -> Result<Vec<u8>, Error> {
        self.send_cmd(Command::Snapshot).await?
    }

    async fn send_set(&self, key: K, value: Option<V>, lww: bool) -> Result<(), Error> {
        self.send_cmd(|sender| Command::Set(key, value, lww, sender))
            .await?
    }

    async fn send_cmd<T>(
        &self,
        cmd: impl FnOnce(oneshot::Sender<T>) -> Command<K, V>,
    ) -> Result<T, Error> {
        let (sender, receiver) = oneshot::channel();

        self.sender.send(cmd(sender)).map_err(|_| Error::Internal)?;

        let val = receiver.map_err(|_| Error::Internal).await?;

        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::executor::block_on;

    fn sleep(millis: u64) {
        use std::thread::sleep;
        use std::time::Duration;

        sleep(Duration::from_millis(millis));
    }

    fn rand_sleep(millis: u64) {
        use rand::{thread_rng, Rng};

        sleep(thread_rng().gen_range(0, millis));
    }

    fn rand_str() -> String {
        use rand::{distributions::Alphanumeric, thread_rng, Rng};
        use std::iter::repeat;

        repeat(())
            .map(|()| thread_rng().sample(Alphanumeric))
            .take(64)
            .collect()
    }

    async fn sync_one(conn: &Connection<String, String>) -> (String, String) {
        let key = rand_str();
        let value = rand_str();

        assert_eq!(conn.set(key.clone(), value.clone()).await, Ok(()));

        (key, value)
    }

    async fn sync_two(
        first_conn: &Connection<String, String>,
        second_conn: &Connection<String, String>,
    ) -> (String, String) {
        let (key, value) = sync_one(first_conn).await;

        while second_conn.get(key.clone()).await.unwrap().as_ref() != Some(&value) {
            sleep(10);
        }

        (key, value)
    }

    fn connect() -> Connection<String, String> {
        Connection::new("localhost:9092".to_owned(), "kkvs_test".to_owned(), None).unwrap()
    }

    async fn connect_and_sync() -> Connection<String, String> {
        let conn = connect();

        sync_one(&conn).await;

        conn
    }

    #[test]
    fn can_connect() {
        connect();
    }

    #[test]
    fn can_set_get_del() {
        block_on(async {
            let conn = connect_and_sync().await;

            let key = "foo".to_owned();
            let value = "bar".to_owned();

            assert_eq!(conn.set(key.clone(), value.clone()).await, Ok(()));

            assert_eq!(conn.get(key.clone()).await, Ok(Some(value)));

            assert_eq!(conn.del(key).await, Ok(()));
        });
    }

    #[test]
    fn can_set() {
        block_on(async {
            let conn = connect_and_sync().await;

            let key = "foobar".to_owned();
            let first_value = "foo".to_owned();
            let second_value = "bar".to_owned();

            assert_eq!(conn.set(key.clone(), first_value.clone()).await, Ok(()));

            assert_eq!(conn.get(key.clone()).await, Ok(Some(first_value)));

            assert_eq!(conn.set(key.clone(), second_value.clone()).await, Ok(()));

            assert_eq!(conn.get(key).await, Ok(Some(second_value)));
        });
    }

    #[test]
    fn can_del() {
        block_on(async {
            let conn = connect_and_sync().await;

            let key = "bar".to_owned();
            let value = "foo".to_owned();

            assert_eq!(conn.set(key.clone(), value.clone()).await, Ok(()));

            assert_eq!(conn.get(key.clone()).await, Ok(Some(value)));

            assert_eq!(conn.del(key.clone()).await, Ok(()));

            assert_eq!(conn.get(key).await, Ok(None));
        });
    }

    #[test]
    fn cannot_del() {
        block_on(async {
            let conn = connect_and_sync().await;

            let key = "barfoo".to_owned();

            assert_eq!(conn.get(key.clone()).await, Ok(None));

            assert_eq!(conn.del(key).await, Err(Error::KeyNotFound));
        });
    }

    async fn perform_racing_writes(
        key: String,
        initial_value: String,
        first_value: String,
        second_value: String,
        clone: bool,
        lww: bool,
    ) -> (String, Result<(), Error>, Result<(), Error>) {
        let first_conn = connect();

        let second_op = {
            let second_conn = if clone { first_conn.clone() } else { connect() };

            sync_two(&first_conn, &second_conn).await;

            assert_eq!(first_conn.set(key.clone(), initial_value).await, Ok(()));

            sync_two(&first_conn, &second_conn).await;

            let key = key.clone();

            spawn(move || {
                block_on(async move {
                    rand_sleep(100);

                    if lww {
                        second_conn.set_lww(key, second_value).await
                    } else {
                        second_conn.set(key, second_value).await
                    }
                })
            })
        };

        rand_sleep(100);

        let first_op = if lww {
            first_conn.set_lww(key.clone(), first_value).await
        } else {
            first_conn.set(key.clone(), first_value).await
        };

        let second_op = second_op.join().unwrap();

        sync_one(&first_conn).await;

        let final_value = first_conn.get(key).await.unwrap().unwrap();

        (final_value, first_op, second_op)
    }

    #[test]
    fn can_detect_conflicting_writes() {
        for &clone in [true, false].iter() {
            let key = "some_key".to_owned();
            let initial_value = "initial_value".to_owned();
            let first_value = "some_value".to_owned();
            let second_value = "some_other_value".to_owned();

            let (final_value, first_op, second_op) = block_on(perform_racing_writes(
                key,
                initial_value,
                first_value.clone(),
                second_value.clone(),
                clone,
                false,
            ));

            assert!(first_op.is_ok() || second_op.is_ok());

            if first_op.is_ok() && second_op.is_ok() {
                assert!(final_value == first_value || final_value == second_value);
            } else if first_op.is_ok() && second_op.is_err() {
                assert_eq!(final_value, first_value);
                assert_eq!(second_op.err().unwrap(), Error::ConflictingWrite);
            } else if first_op.is_err() && second_op.is_ok() {
                assert_eq!(first_op.err().unwrap(), Error::ConflictingWrite);
                assert_eq!(final_value, second_value);
            }
        }
    }

    #[test]
    fn can_use_last_write_wins() {
        for &clone in [true, false].iter() {
            let key = "some_other_key".to_owned();
            let initial_value = "initial_value".to_owned();
            let first_value = "some_value".to_owned();
            let second_value = "some_other_value".to_owned();

            let (final_value, first_op, second_op) = block_on(perform_racing_writes(
                key,
                initial_value,
                first_value.clone(),
                second_value.clone(),
                clone,
                true,
            ));

            assert_eq!(first_op, Ok(()));
            assert_eq!(second_op, Ok(()));
            assert!(final_value == first_value || final_value == second_value);
        }
    }
}
