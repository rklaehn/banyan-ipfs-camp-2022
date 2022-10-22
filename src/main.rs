use std::time::Instant;

use banyan::{*, store::{BranchCache, ReadOnlyStore, BlockWriter}};
use banyan_utils::tag_index::TagSet as ActyxTagSet;
use banyan_utils::tags::{TT as ActyxTT, Sha256Digest as ActyxLink};
use banyan_utils::tags::Key as ActyxKey;

/// An example that is close to how banyan is used at actyx
///
/// The tree types are not exactly the same, the query capabilities in actyx are much more advances, but the general idea is the same.
fn actyx_example(store: impl ReadOnlyStore<ActyxLink> + BlockWriter<ActyxLink>) -> anyhow::Result<()> {
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
    let t0 = Instant::now();
    let mut sum = 0;
    // iterate over all (offset, key, value) triples of the tree
    for item in txn.iter_from(&tree) {
        let (_i, _k, v) = item?;
        sum += v;
        // println!("{} {:?} {}", i, k, v);
    }
    println!("{} {}s", sum, t0.elapsed().as_secs_f64());
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // create a store that reads and writes from ipfs
    let store = banyan_utils::ipfs::IpfsStore::new()?;
    // create a store that reads and writes from memory
    // let store = banyan::store::MemStore::new(1000000000, ActyxLink::digest);
    actyx_example(store)?;
    Ok(())
}
