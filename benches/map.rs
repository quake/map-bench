use rayon::prelude::*;
use lazy_static::lazy_static;
use rand::{random, thread_rng, seq::SliceRandom};
use criterion::{Criterion, criterion_group, criterion_main};

type Key = [u8; 32];
type Value = [u64; 32];

lazy_static! {
    static ref KEYS: Vec<Key> = random_keys();
    static ref SHUFFLED_KEYS: Vec<Key> = shuffle_random_keys();
    static ref RANDOM_VALUE: Value = random();
    static ref STD_RWLOCK_HASHMAP: std::sync::RwLock<std::collections::HashMap<Key, Value>> = std_rwlock_hashmap_insert();
    static ref PARKING_LOT_RWLOCK_HASHMAP: parking_lot::RwLock<std::collections::HashMap<Key, Value>> = parking_lot_rwlock_hashmap_insert();
    static ref CHASHMAP: chashmap::CHashMap<Key, Value> = chashmap_insert();
    static ref CONTRIE_CONMAP: contrie::ConMap<Key, Value> = contrie_conmap_insert();
    static ref CCL_DHASHMAP: ccl::dhashmap::DHashMap<Key, Value> = ccl_dhashmap_insert();
    static ref SKIPLIST_MAP: crossbeam_skiplist::SkipMap<Key, Value> = skiplist_skipmap_insert();
    // static ref KUDZU_MAP: kudzu::Map<Key, Value> = kudzu_map_insert();
}

fn random_keys() -> Vec<Key> {
    (0..10000).map(|_| random()).collect::<Vec<Key>>()
}

fn shuffle_random_keys() -> Vec<Key> {
    let mut keys = KEYS.clone();
    let mut rng = thread_rng();
    keys.shuffle(&mut rng);
    keys
}

// std rwlock + std hashmap
fn std_rwlock_hashmap_insert() -> std::sync::RwLock<std::collections::HashMap<Key, Value>> {
    let map = std::sync::RwLock::new(std::collections::HashMap::with_capacity(KEYS.len()));
    KEYS.clone().into_par_iter().for_each(|key| {
        map.write().unwrap().insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn std_rwlock_hashmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(STD_RWLOCK_HASHMAP.read().unwrap().get(key).is_some());
    });
}

fn std_rwlock_hashmap_insert_get() {
    let map = std::sync::RwLock::new(std::collections::HashMap::with_capacity(KEYS.len()));
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.write().unwrap().insert(key, RANDOM_VALUE.clone());
        map.read().unwrap().get(&SHUFFLED_KEYS[i]);
    });
}

// parking lot rwlock + std hashmap
fn parking_lot_rwlock_hashmap_insert() -> parking_lot::RwLock<std::collections::HashMap<Key, Value>> {
    let map = parking_lot::RwLock::new(std::collections::HashMap::with_capacity(KEYS.len()));
    KEYS.clone().into_par_iter().for_each(|key| {
        map.write().insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn parking_lot_rwlock_hashmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(PARKING_LOT_RWLOCK_HASHMAP.read().get(key).is_some());
    });
}

fn parking_lot_rwlock_hashmap_insert_get() {
    let map = parking_lot::RwLock::new(std::collections::HashMap::with_capacity(KEYS.len()));
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.write().insert(key, RANDOM_VALUE.clone());
        map.read().get(&SHUFFLED_KEYS[i]);
    });
}

// kudzu map (require nightly rust)
// fn kudzu_map_insert() -> kudzu::Map<Key, Value> {
//     let map = kudzu::Map::new();
//     KEYS.clone().into_par_iter().for_each(|key| {
//         map.insert(key, RANDOM_VALUE.clone());
//     });
//     map
// }

// chashmap
fn chashmap_insert() -> chashmap::CHashMap<Key, Value> {
    let map = chashmap::CHashMap::with_capacity(KEYS.len());
    KEYS.clone().into_par_iter().for_each(|key| {
        map.insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn chashmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(CHASHMAP.get(key).is_some());
    });
}

fn chashmap_insert_get() {
    let map = chashmap::CHashMap::with_capacity(KEYS.len());
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.insert(key, RANDOM_VALUE.clone());
        map.get(&SHUFFLED_KEYS[i]);
    });
}

// contrie conmap
fn contrie_conmap_insert() -> contrie::ConMap<Key, Value> {
    let map = contrie::ConMap::new();
    KEYS.clone().into_par_iter().for_each(|key| {
        map.insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn contrie_conmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(CONTRIE_CONMAP.get(key).is_some());
    });
}

fn contrie_conmap_insert_get() {
    let map = contrie::ConMap::new();
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.insert(key, RANDOM_VALUE.clone());
        map.get(&SHUFFLED_KEYS[i]);
    });
}

// ccl dhashmap
fn ccl_dhashmap_insert() -> ccl::dhashmap::DHashMap<Key, Value> {
    let map = ccl::dhashmap::DHashMap::default();
    KEYS.clone().into_par_iter().for_each(|key| {
        map.insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn ccl_dhashmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(CCL_DHASHMAP.get(key).is_some());
    });
}

fn ccl_dhashmap_insert_get() {
    let map = ccl::dhashmap::DHashMap::default();
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.insert(key, RANDOM_VALUE.clone());
        map.get(&SHUFFLED_KEYS[i]);
    });
}

// skiplist skipmap
fn skiplist_skipmap_insert() -> crossbeam_skiplist::SkipMap<Key, Value> {
    let map = crossbeam_skiplist::SkipMap::new();
    KEYS.clone().into_par_iter().for_each(|key| {
        map.insert(key, RANDOM_VALUE.clone());
    });
    map
}

fn skiplist_skipmap_get() {
    SHUFFLED_KEYS.par_iter().for_each(|key| {
        assert!(SKIPLIST_MAP.get(key).is_some());
    });
}

fn skiplist_skipmap_insert_get() {
    let map = crossbeam_skiplist::SkipMap::new();
    KEYS.clone().into_par_iter().enumerate().for_each(|(i, key)| {
        map.insert(key, RANDOM_VALUE.clone());
        map.get(&SHUFFLED_KEYS[i]);
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // insert
    c.bench_function("parking_lot_rwlock_hashmap_insert", |b| {
        b.iter(|| parking_lot_rwlock_hashmap_insert())
    });

    c.bench_function("chashmap_insert", |b| {
        b.iter(|| chashmap_insert())
    });

    c.bench_function("contrie_conmap_insert", |b| {
        b.iter(|| contrie_conmap_insert())
    });

    c.bench_function("ccl_dhashmap_insert", |b| {
        b.iter(|| ccl_dhashmap_insert())
    });

    c.bench_function("skiplist_skipmap_insert", |b| {
        b.iter(|| skiplist_skipmap_insert())
    });

    // get
    c.bench_function("parking_lot_rwlock_hashmap_get", |b| {
        b.iter(|| parking_lot_rwlock_hashmap_get())
    });

    c.bench_function("chashmap_get", |b| {
        b.iter(|| chashmap_get())
    });

    c.bench_function("contrie_conmap_get", |b| {
        b.iter(|| contrie_conmap_get())
    });

    c.bench_function("ccl_dhashmap_get", |b| {
        b.iter(|| ccl_dhashmap_get())
    });

    c.bench_function("skiplist_skipmap_get", |b| {
        b.iter(|| skiplist_skipmap_get())
    });

    // insert and get
    c.bench_function("parking_lot_rwlock_hashmap_insert_get", |b| {
        b.iter(|| parking_lot_rwlock_hashmap_insert_get())
    });

    c.bench_function("chashmap_insert_get", |b| {
        b.iter(|| chashmap_insert_get())
    });

    c.bench_function("contrie_conmap_insert_get", |b| {
        b.iter(|| contrie_conmap_insert_get())
    });

    c.bench_function("ccl_dhashmap_insert_get", |b| {
        b.iter(|| ccl_dhashmap_insert_get())
    });

    c.bench_function("skiplist_skipmap_insert_get", |b| {
        b.iter(|| skiplist_skipmap_insert_get())
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
