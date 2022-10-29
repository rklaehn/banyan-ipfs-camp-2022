#![allow(clippy::redundant_clone)]
use std::time::Instant;

use banyan::{
    store::{BlockWriter, BranchCache, ReadOnlyStore},
    *,
};
use banyan_utils::tags::Sha256Digest;

/// Example to use banyan as just an efficient compressed event sequence without any indexes
///
/// You will only be able to access by index or query/stream by index range
fn sequence_example(
    store: impl ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest>,
) -> anyhow::Result<()> {
    let n = 1000000u64;
    println!("Example 1: building sequence of {} blocks on banyan", n);

    #[derive(Debug, Clone)]
    struct SimpleTT;

    impl banyan::TreeTypes for SimpleTT {
        type Key = (); // no keys
        type Summary = (); // no summaries
        type KeySeq = banyan::index::UnitSeq; // a sequence of unit keys
        type SummarySeq = banyan::index::UnitSeq; // a sequence of unit summaries
        type Link = Sha256Digest; // use a 32 byte sha256 digest as link
        const NONCE: &'static [u8; 24] = b"Simple example for camp.";
    }

    // create some data in advance
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
    println!("{:#?} {}s", tree, t0.elapsed().as_secs_f64());

    // reading
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{}", sum);
    println!();
    Ok(())
}

/// Example to use banyan as just an efficient compressed event sequence without any indexes
///
/// You will only be able to access by index or query/stream by index range
fn custom_index_example(
    store: impl ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest>,
) -> anyhow::Result<()> {
    let n = 1000000u64;
    println!(
        "Example 1: building sequence of {} blocks with custom index on banyan",
        n
    );

    #[derive(Debug, Clone)]
    struct IndexTT;

    #[derive(Debug, Clone, PartialEq, Eq, libipld::DagCbor)]
    struct KeyRange {
        /// inclusive
        min: u64,
        /// inclusive
        max: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RangeQuery {
        /// inclusive
        min: u64,
        /// inclusive
        max: u64,
    }

    impl banyan::query::Query<IndexTT> for RangeQuery {
        fn containing(&self, _offset: u64, index: &index::LeafIndex<IndexTT>, res: &mut [bool]) {
            let keys = index.keys.as_ref();
            for (i, key) in keys.iter().enumerate() {
                // true if the key is within range
                res[i] = *key >= self.min && *key <= self.max;
            }
        }

        fn intersecting(
            &self,
            _offset: u64,
            index: &index::BranchIndex<IndexTT>,
            res: &mut [bool],
        ) {
            let summaries = index.summaries.as_ref();
            for (i, summary) in summaries.iter().enumerate() {
                // true if the summary range intersects with the query range
                res[i] = !(summary.min > self.max || summary.max < self.min);
            }
        }
    }

    // define our custom tree types
    impl banyan::TreeTypes for IndexTT {
        type Key = u64; // key is an integer (e.g. a time or a value)
        type Summary = KeyRange; // no summaries
        type KeySeq = banyan::index::VecSeq<u64>; // a sequence of unit keys
        type SummarySeq = banyan::index::VecSeq<KeyRange>; // a sequence of unit summaries
        type Link = Sha256Digest; // use a 32 byte sha256 digest as link
        const NONCE: &'static [u8; 24] = b"Complex example for camp";
    }

    /// Define how to create a summary from a sequence of values
    impl banyan::index::Summarizable<KeyRange> for banyan::index::VecSeq<u64> {
        fn summarize(&self) -> KeyRange {
            let min = self.as_ref().iter().cloned().min().unwrap_or_default();
            let max = self.as_ref().iter().cloned().max().unwrap_or_default();
            KeyRange { min, max }
        }
    }

    /// Define how to create a summary from a sequence of summaries
    impl banyan::index::Summarizable<KeyRange> for banyan::index::VecSeq<KeyRange> {
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
    println!("{:#?} {}s", tree, t0.elapsed().as_secs_f64());

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
    let mut n = 0;
    for item in txn.iter_filtered(
        &tree,
        RangeQuery {
            min: 500,
            max: 1000,
        },
    ) {
        let (_i, _k, v) = item?;
        // println!("{} {:?} {}", i, k, v);
        sum += v;
        n += 1;
    }
    println!("{} {}", sum, n);
    println!();
    Ok(())
}

/// An example that is close to how banyan is used at actyx
///
/// The tree types are not exactly the same, the query capabilities in actyx are much more advances, but the general idea is the same.
fn actyx_example(
    store: impl ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest>,
) -> anyhow::Result<()> {
    let n = 1000000u64;
    println!(
        "Example 1: building sequence of {} blocks with actyx style index on banyan",
        n
    );

    use banyan_utils::tag_index::TagSet as ActyxTagSet;
    use banyan_utils::tags::{Key as ActyxKey, TT as ActyxTT};

    // create some data in advance
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
    println!("{:#?} {}s", tree, t0.elapsed().as_secs_f64());

    // reading
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{}", sum);
    println!();
    Ok(())
}

fn run(store: impl ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest>) -> anyhow::Result<()> {
    sequence_example(store.clone())?;
    custom_index_example(store.clone())?;
    actyx_example(store.clone())?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // create a store that reads and writes from ipfs. Requires kubo (go-ipfs) compatible API on port 5001
    let mut store = banyan_utils::ipfs::IpfsStore::new()?;
    match store.put(vec![]) {
        Ok(_) => {
            println!("kubo seems to be available. Using kubo interface on port 5001");
            run(store)
        }
        Err(_) => {
            println!("kubo seems not to be available. Using in memory store");
            let store = banyan::store::MemStore::new(1000000000, Sha256Digest::digest);
            run(store)
        }
    }
}
