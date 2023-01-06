#[cfg(test)]
mod tests;

use std::{collections::HashMap, any::Any, thread, sync::{mpsc::{Receiver, SyncSender}, Arc, RwLock}, time::{Instant, Duration}, fmt::Display};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq)]
pub enum Error {
	Expired,
	InvalidCast,
	NoValue,
	ThreadErr(String),
	ThreadDisconnected,
}

type Cacheable = Arc<dyn Any + Send + Sync>;

struct CacheItem {
	data: Cacheable,
	expires: Instant,
	ttl_ms: u64,
}

enum CacheCommand {
	Get(String, tokio::sync::mpsc::Sender<Result<Cacheable>>, bool),
	Put(String, Cacheable, u64),
	Remove(String),
}

static CACHE_SEND: RwLock<Option<SyncSender<CacheCommand>>> = RwLock::new(None);

pub async fn init(command_buffer_size: Option<usize>) -> Result<()> {
	// Create a comms channel to send from anywhere to the cache thread
	let (send, recv) = std::sync::mpsc::sync_channel(
		command_buffer_size.unwrap_or(128)
	);

	let mut write = CACHE_SEND.write().expect("[MEMCACHE] Cache send is poisoned");
	if write.is_some() {
		panic!("[MEMCACHE] Init must only be called once");
	}

	*write = Some(send);

	// Start the cache thread
	thread::Builder::new()
		.name(String::from("memcache"))
		.spawn(move || run(recv))
		.map_err(|e| Error::ThreadErr(e.to_string()))?;

	Ok(())
}

pub async fn get<T: Any + Send + Sync>(key: String) -> Result<Arc<T>> {
	let sender = get_sender();
	let (send, recv) = tokio::sync::mpsc::channel(1);
	let command = CacheCommand::Get(key.to_string(), send, false);
	sender.send(command).map_err(|e| Error::ThreadErr(e.to_string()))?;

	ret_val(recv).await
}

pub async fn get_refresh<T: Any + Send + Sync>(key: String) -> Result<Arc<T>> {
	let sender = get_sender();
	let (send, recv) = tokio::sync::mpsc::channel(1);
	let command = CacheCommand::Get(key.to_string(), send, true);
	sender.send(command).map_err(|e| Error::ThreadErr(e.to_string()))?;

	ret_val(recv).await
}

pub async fn put<T: Any + Send + Sync>(key: String, value: T, ttl_ms: u64) -> Result<()> {
	let sender = get_sender();
	let command = CacheCommand::Put(key.to_string(), Arc::new(value), ttl_ms);
	sender.send(command).map_err(|e| Error::ThreadErr(e.to_string()))
}

pub async fn remove(key: String) -> Result<()> {
	let sender = get_sender();
	let command = CacheCommand::Remove(key.to_string());
	sender.send(command).map_err(|e| Error::ThreadErr(e.to_string()))
}

fn get_sender() -> SyncSender<CacheCommand> {
	CACHE_SEND.read()
		.expect("[MEMCACHE] Cache send is poisoned")
		.as_ref()
		.expect("[MEMCACHE] Not initialized")
		.clone()
}

async fn ret_val<T: Any + Send + Sync>(mut recv: tokio::sync::mpsc::Receiver<Result<Cacheable>>) -> Result<Arc<T>> {
	recv.recv().await
		.ok_or(Error::ThreadDisconnected)??
		.downcast()
		.map_err(|_| Error::InvalidCast)
}

fn run(receiver: Receiver<CacheCommand>) {
	let mut cache: HashMap<String, CacheItem> = HashMap::new();
	let mut min_expire = Instant::now();

	loop {
		// Wait for a command
		let command = match receiver.recv() {
			Ok(v) => v,
			Err(_) => {
				println!("[MEMCACHE] Sender has disconnected; stopping cache...");
				break;
			}
		};

		let now = Instant::now();
		match command {
			CacheCommand::Get(key, send, refresh) => {
				let item = match cache.get(&key) {
					Some(v) => v,
					None => {
						send.blocking_send(Err(Error::NoValue)).unwrap();
						continue;
					}
				};

				if item.expires <= now {
					send.blocking_send(Err(Error::Expired)).unwrap();
					cache.remove(&key);
					continue;
				}

				let value = Arc::clone(&item.data);
				send.blocking_send(Ok(value)).unwrap();

				if refresh {
					let item = cache.get_mut(&key).unwrap();
					item.expires = now + Duration::from_millis(item.ttl_ms);
				}
			},
			CacheCommand::Put(key, value, ttl_ms) => {
				let item = CacheItem {
					data: value,
					expires: now + Duration::from_millis(ttl_ms),
					ttl_ms,
				};

				cache.insert(key, item);
			},
			CacheCommand::Remove(key) => {
				cache.remove(&key);
			}
		}

		if now > min_expire {
			if let Some(v) = cleanup(&mut cache) {
				min_expire = v;
			}
		}
	}
}

fn cleanup(cache: &mut HashMap<String, CacheItem>) -> Option<Instant> {
	let mut expired = Vec::new();
	let mut min_expire = None;
	let now = Instant::now();
	for (key, value) in cache.iter() {
		if value.expires <= now {
			expired.push(key.clone());
		} else if min_expire.is_none() || min_expire.unwrap() > value.expires {
			min_expire = Some(value.expires);
		}
	}

	let clean_count = expired.len();
	for key in expired {
		cache.remove(&key);
	}

	println!("[MEMCACHE] GC collected {clean_count} items.");
	min_expire
}

impl Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self {
			Error::Expired => write!(f, "Value expired"),
			Error::InvalidCast => write!(f, "Invalid cast"),
			Error::NoValue => write!(f, "No value"),
			Error::ThreadDisconnected => write!(f, "Thread disconnected"),
			Error::ThreadErr(e) => write!(f, "Thread disconnected: {}", e),
		}
	}
}
