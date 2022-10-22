#![allow(clippy::redundant_clone)]
use std::{ops::Range, time::Instant};

use banyan::{
    index::{Summarizable, UnitSeq},
    store::{BlockWriter, BranchCache, ReadOnlyStore},
    *,
};
use banyan_utils::tag_index::TagSet as ActyxTagSet;
use banyan_utils::tags::Key as ActyxKey;
use banyan_utils::tags::{Sha256Digest as ActyxLink, Sha256Digest, TT as ActyxTT};

/// An example that is close to how banyan is used at actyx
///
/// The tree types are not exactly the same, the query capabilities in actyx are much more advances, but the general idea is the same.
fn actyx_example(
    store: impl ReadOnlyStore<ActyxLink> + BlockWriter<ActyxLink>,
) -> anyhow::Result<()> {
    // create some data in advance
    let n = 1000000;
    let xs = (0..n)
        .map(|i| (ActyxKey::single(i, i, ActyxTagSet::empty()), i))
        .collect::<Vec<_>>();

    // setup
    // create a forest with the actyx tree types
    let forest = Forest::<ActyxTT, _>::new(store.clone(), BranchCache::new(1024));
    // configure a tree builder with a reasonable tree config and default secrets (not secure)
    let mut builder = StreamBuilder::new(Config::debug_fast(), Secrets::default());
    // open a transaction.
    // Since we use the same store for reading and for writing, we don't have to commit the transaction
    // it is just a pair of a block reader and a block writer
    let mut txn = Transaction::new(forest, store);

    // writing
    let t0 = Instant::now();
    // in the transaction, add to the builder from the vec
    txn.extend(&mut builder, xs)?;
    // take a snapshot of the builder. We are writing straight to ipfs, so no need to commit the txn
    let tree = builder.snapshot();
    // now we have a persistent tree
    println!("{:?} {}s", tree, t0.elapsed().as_secs_f64());

    // reading
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{}", sum);
    Ok(())
}

/// Example to use banyan as just an efficient compressed event sequence without any indexes
///
/// You will only be able to access by index or query/stream by index range
fn sequence_example(
    store: impl ReadOnlyStore<ActyxLink> + BlockWriter<ActyxLink>,
) -> anyhow::Result<()> {
    #[derive(Debug, Clone)]
    struct SimpleTT;

    impl banyan::TreeTypes for SimpleTT {
        type Key = (); // no keys
        type Summary = (); // no summaries
        type KeySeq = UnitSeq; // a sequence of unit keys
        type SummarySeq = UnitSeq; // a sequence of unit summaries
        type Link = Sha256Digest; // use a 32 byte sha256 digest as link
        const NONCE: &'static [u8; 24] = b"Simple example for camp.";
    }

    // create some data in advance
    let n = 1000000u64;
    let xs = (0..n).map(|i| ((), i)).collect::<Vec<_>>();

    // setup
    // create a forest
    let forest = Forest::<SimpleTT, _>::new(store.clone(), BranchCache::new(1024));
    // configure a tree builder with a reasonable tree config and default secrets (not secure)
    let mut builder = StreamBuilder::new(Config::debug_fast(), Secrets::default());
    // open a transaction.
    // Since we use the same store for reading and for writing, we don't have to commit the transaction
    // it is just a pair of a block reader and a block writer
    let mut txn = Transaction::new(forest, store);

    // writing
    let t0 = Instant::now();
    // in the transaction, add to the builder from the vec
    txn.extend(&mut builder, xs)?;
    // take a snapshot of the builder. We are writing straight to ipfs, so no need to commit the txn
    let tree = builder.snapshot();
    // now we have a persistent tree
    println!("{:?} {}s", tree, t0.elapsed().as_secs_f64());

    // reading
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{}", sum);
    Ok(())
}

/// Example to use banyan as just an efficient compressed event sequence without any indexes
///
/// You will only be able to access by index or query/stream by index range
fn custom_index_example(
    store: impl ReadOnlyStore<ActyxLink> + BlockWriter<ActyxLink>,
) -> anyhow::Result<()> {
    #[derive(Debug, Clone)]
    struct IndexTT;

    #[derive(Debug, Clone, PartialEq, Eq, libipld::DagCbor)]
    struct KeyRange {
        min: u64, // inclusive
        max: u64, // inclusive
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RangeQuery {
        min: u64, // inclusive
        max: u64, // inclusive
    }

    impl banyan::query::Query<IndexTT> for RangeQuery {
        fn containing(&self, offset: u64, index: &index::LeafIndex<IndexTT>, res: &mut [bool]) {
            let keys = index.keys.as_ref();
            for (i, key) in keys.iter().enumerate() {
                // true if the key is within range
                res[i] = *key >= self.min && *key <= self.max;
            }
        }

        fn intersecting(&self, offset: u64, index: &index::BranchIndex<IndexTT>, res: &mut [bool]) {
            let summaries = index.summaries.as_ref();
            for (i, summary) in summaries.iter().enumerate() {
                // true if the summary range intersects with the query range
                res[i] = !(summary.min > self.max || summary.max < self.min);
            }
        }
    }

    impl banyan::TreeTypes for IndexTT {
        type Key = u64; // key is an integer (e.g. a time or a value)
        type Summary = KeyRange; // no summaries
        type KeySeq = banyan::index::VecSeq<u64>; // a sequence of unit keys
        type SummarySeq = banyan::index::VecSeq<KeyRange>; // a sequence of unit summaries
        type Link = Sha256Digest; // use a 32 byte sha256 digest as link
        const NONCE: &'static [u8; 24] = b"Complex example for camp";
    }

    impl Summarizable<KeyRange> for banyan::index::VecSeq<u64> {
        fn summarize(&self) -> KeyRange {
            let min = self.as_ref().iter().cloned().min().unwrap_or_default();
            let max = self.as_ref().iter().cloned().max().unwrap_or_default();
            KeyRange { min, max }
        }
    }

    impl Summarizable<KeyRange> for banyan::index::VecSeq<KeyRange> {
        fn summarize(&self) -> KeyRange {
            let min = self
                .as_ref()
                .iter()
                .map(|x| x.min)
                .min()
                .unwrap_or_default();
            let max = self
                .as_ref()
                .iter()
                .map(|x| x.max)
                .max()
                .unwrap_or_default();
            KeyRange { min, max }
        }
    }

    // create some data in advance
    let n = 1000000u64;
    let xs = (0..n).map(|i| (i, i)).collect::<Vec<_>>();

    // setup
    // create a forest
    let forest = Forest::<IndexTT, _>::new(store.clone(), BranchCache::new(1024));
    // configure a tree builder with a reasonable tree config and default secrets (not secure)
    let mut builder = StreamBuilder::new(Config::debug_fast(), Secrets::default());
    // open a transaction.
    // Since we use the same store for reading and for writing, we don't have to commit the transaction
    // it is just a pair of a block reader and a block writer
    let mut txn = Transaction::new(forest, store);

    // writing
    let t0 = Instant::now();
    // in the transaction, add to the builder from the vec
    txn.extend(&mut builder, xs)?;
    // take a snapshot of the builder. We are writing straight to ipfs, so no need to commit the txn
    let tree = builder.snapshot();
    // now we have a persistent tree
    println!("{:?} {}s", tree, t0.elapsed().as_secs_f64());

    // reading
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{}", sum);

    // querying
    let mut sum = 0;
    for item in txn.iter_filtered(
        &tree,
        RangeQuery {
            min: 100,
            max: 1000,
        },
    ) {
        let (i, k, v) = item?;
        println!("{} {:?} {}", i, k, v);
        sum += v;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // create a store that reads and writes from ipfs. Requires kubo (go-ipfs) on port 5001
    // let store = banyan_utils::ipfs::IpfsStore::new()?;
    // create a store that reads and writes from memory
    let store = banyan::store::MemStore::new(1000000000, ActyxLink::digest);
    actyx_example(store.clone())?;

    sequence_example(store.clone())?;

    custom_index_example(store.clone())?;
    Ok(())
}
