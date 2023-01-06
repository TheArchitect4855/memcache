use std::{thread, time::Duration};
use rand::Rng;

use crate::Error;

#[tokio::test]
async fn test_init() {
	crate::init(None).await.unwrap();
	test_get_put().await;
	test_get_refresh().await;
	test_remove().await;
	test_gc().await;
}

async fn test_get_put() {
	// Try to get a value that doesn't exist
	let err = crate::get::<i32>(String::from(String::from("foo")))
		.await
		.unwrap_err();

	assert_eq!(err, Error::NoValue);

	// Add a value
	crate::put(String::from(String::from("foo")), 69, 1000)
		.await
		.unwrap();

	// Try to get the value with the incorrect type
	let err = crate::get::<String>(String::from(String::from("foo")))
		.await
		.unwrap_err();
	
	assert_eq!(err, Error::InvalidCast);

	// Get the value with the correct type
	let val = crate::get::<i32>(String::from("foo"))
		.await
		.unwrap();

	assert_eq!(*val, 69);

	// Wait for the value to expire
	thread::sleep(Duration::from_millis(1000));

	// Try to get an expired value
	let err = crate::get::<i32>(String::from("foo"))
		.await
		.unwrap_err();

	assert_eq!(err, Error::Expired);
}

async fn test_get_refresh() {
	crate::put(String::from(String::from("bar")), 1337, 1000)
		.await
		.unwrap();
	
	// Wait 250 ms, refresh the value
	thread::sleep(Duration::from_millis(250));
	let val = crate::get_refresh::<i32>(String::from("bar"))
		.await
		.unwrap();
	
	assert_eq!(*val, 1337);

	// Wait another 500 ms, check if the value is valid
	thread::sleep(Duration::from_millis(500));
	let val = crate::get::<i32>(String::from("bar"))
		.await
		.unwrap();

	assert_eq!(*val, 1337);
}

async fn test_remove() {
	crate::put(String::from(String::from("baz")), 42, 86_400_000)
		.await
		.unwrap();

	// Get the value
	let val = crate::get::<i32>(String::from("baz"))
		.await
		.unwrap();

	assert_eq!(*val, 42);

	// Remove the value
	crate::remove(String::from("baz"))
		.await
		.unwrap();

	// Make sure it doesn't exist
	let err = crate::get::<i32>(String::from("baz"))
		.await
		.unwrap_err();

	assert_eq!(err, Error::NoValue);
}

async fn test_gc() {
	// Create items
	const N: usize = 1000;
	static mut KEYS: Vec<String> = Vec::new();

	let letters: Vec<char> = "abcdefghijklmnopqrstuvwxyz1234567890".chars().collect();
	let mut key_buf = String::with_capacity(16);
	for _ in 0..N {
		for _ in 0..key_buf.capacity() {
			let ci = rand::thread_rng().gen_range(0..letters.len());
			let c = letters[ci];
			key_buf.push(c);
		}

		unsafe {
			KEYS.push(key_buf.clone());
		}

		let val: usize = rand::random();
		let ttl = rand::thread_rng().gen_range(1..1000);
		crate::put(key_buf.clone(), val, ttl).await.unwrap();
		key_buf.clear();
	}

	// Test
	for _ in 0..100 {
		unsafe {
			for key in &KEYS {
				let _ = crate::get::<usize>(key.to_string());
			}
		}

		thread::sleep(Duration::from_millis(10));
	}
}
