package iavl

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	tmdb "github.com/line/tm-db/v2"
	"github.com/line/tm-db/v2/goleveldb"
	"github.com/pkg/errors"
)

const (
	int64Size = 8
	hashSize  = sha256.Size
)

// cache hit ratio
var (
	nodeCacheHit              int64
	nodeCacheMiss             int64
	totalCount, totalSize     int64
	accCount, accSize         int64
	bankCount, bankSize       int64
	stakingCount, stakingSize int64
)

func GetNodeCacheStats() []int64 {
	r := make([]int64, 10)
	r[0] = nodeCacheHit
	r[1] = nodeCacheMiss
	r[2] = totalCount
	r[3] = totalSize
	r[4] = accCount
	r[5] = accSize
	r[6] = bankCount
	r[7] = bankSize
	r[8] = stakingCount
	r[9] = stakingSize
	return r
}

func ResetNodeCacheStats() {
	nodeCacheHit, nodeCacheMiss = 0, 0
	totalCount, totalSize = 0, 0
	accCount, accSize = 0, 0
	bankCount, bankSize = 0, 0
	stakingCount, stakingSize = 0, 0
}

var (
	// All node keys are prefixed with the byte 'n'. This ensures no collision is
	// possible with the other keys, and makes them easier to traverse. They are indexed by the node hash.
	nodeKeyFormat = NewKeyFormat('n', hashSize) // n<hash>

	// Orphans are keyed in the database by their expected lifetime.
	// The first number represents the *last* version at which the orphan needs
	// to exist, while the second number represents the *earliest* version at
	// which it is expected to exist - which starts out by being the version
	// of the node being orphaned.
	orphanKeyFormat = NewKeyFormat('o', int64Size, int64Size, hashSize) // o<last-version><first-version><hash>

	// Root nodes are indexed separately by their version
	rootKeyFormat = NewKeyFormat('r', int64Size) // r<version>
)

type Cache interface {
	Set(key, value []byte)
	Has(key []byte) bool
	Get(dst, key []byte) []byte
	Del(key []byte)
}

type nodeDB struct {
	mtx            sync.Mutex       // Read/write lock.
	db             tmdb.DB          // Persistent node storage.
	batch          tmdb.Batch       // Batched writing buffer.
	opts           Options          // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers
	latestVersion  int64
	nodeCache      Cache // Node cache.
	kind           int
}

func newNodeDB(db tmdb.DB, cacheSize int, opts *Options) *nodeDB {
	var cache Cache
	if cacheSize > 0 {
		cache = fastcache.New(cacheSize)
	}
	return newNodeDBWithCache(db, cache, opts)
}

func newNodeDBWithCache(db tmdb.DB, cache Cache, opts *Options) *nodeDB {
	if opts == nil {
		o := DefaultOptions()
		opts = &o
	}
	ndb := &nodeDB{
		db:             db,
		batch:          db.NewBatch(),
		opts:           *opts,
		latestVersion:  0, // initially invalid
		nodeCache:      cache,
		versionReaders: make(map[int64]uint32, 8),
	}
	launch_depth := 2 // 4 threads * (acc & bank) -> 8 threads
	if val := os.Getenv("USE_PRELOAD"); len(val) > 0 {
		if depth, err := strconv.Atoi(val); err != nil {
			launch_depth = 0
		} else {
			launch_depth = depth
		}
	}
	if 0 < launch_depth && launch_depth <= 10 {
		go ndb.preload(nil, launch_depth)
	}
	return ndb
}

// GetNode gets a node from memory or disk. If it is an inner node, it does not
// load its children.
func (ndb *nodeDB) GetNode(hash []byte) *Node {
	// ndb.mtx.Lock()
	// defer ndb.mtx.Unlock()

	if len(hash) == 0 {
		panic("nodeDB.GetNode() requires hash")
	}

	// Check the cache.
	if ndb.nodeCache != nil {
		value := ndb.nodeCache.Get(nil, hash)
		if value != nil {
			node, err := MakeNode(value)
			if err != nil {
				panic(fmt.Sprintf("can't get node %X: %v", hash, err))
			}
			node.hash = hash
			node.persisted = true
			atomic.AddInt64(&nodeCacheHit, 1)
			return node
		}
		atomic.AddInt64(&nodeCacheMiss, 1)
	}

	// Doesn't exist, load.
	buf, err := ndb.db.Get(ndb.nodeKey(hash))
	if err != nil {
		panic(fmt.Sprintf("can't get node %X: %v", hash, err))
	}
	if buf == nil {
		panic(fmt.Sprintf("Value missing for hash %x corresponding to nodeKey %x", hash, ndb.nodeKey(hash)))
	}

	node, err := MakeNode(buf)
	if err != nil {
		panic(fmt.Sprintf("Error reading Node. bytes: %x, error: %v", buf, err))
	}

	node.hash = hash
	node.persisted = true
	go ndb.cacheNode(hash, buf)

	return node
}

// SaveNode saves a node to disk.
func (ndb *nodeDB) SaveNode(node *Node) {
	// ndb.mtx.Lock()
	// defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.encodedSize())

	if err := node.writeBytes(&buf); err != nil {
		panic(err)
	}

	if err := ndb.batch.Set(ndb.nodeKey(node.hash), buf.Bytes()); err != nil {
		panic(err)
	}
	debug("BATCH SAVE %X %p\n", node.hash, node)
	node.persisted = true
	ndb.cacheNode(node.hash, buf.Bytes())
}

func (ndb *nodeDB) saveNodeEx(batch tmdb.Batch, node *Node) {
	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.encodedSize())

	if err := node.writeBytes(&buf); err != nil {
		panic(err)
	}

	if err := batch.Set(ndb.nodeKey(node.hash), buf.Bytes()); err != nil {
		panic(err)
	}
	debug("BATCH SAVE %X %p\n", node.hash, node)
	node.persisted = true
	go ndb.cacheNode(node.hash, buf.Bytes())
}

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)

	if ldb, ok := ndb.db.(*goleveldb.GoLevelDB); ok {
		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			return false, err
		}
		return exists, nil
	}
	value, err := ndb.db.Get(key)
	if err != nil {
		return false, err
	}

	return value != nil, nil
}

// SaveBranch saves the given node and all of its descendants.
// NOTE: This function clears leftNode/rigthNode recursively and
// calls _hash() on the given node.
// TODO refactor, maybe use hashWithCount() but provide a callback.
func (ndb *nodeDB) SaveBranch(node *Node) []byte {
	if node.persisted {
		return node.hash
	}

	if node.leftNode != nil {
		node.leftHash = ndb.SaveBranch(node.leftNode)
	}
	if node.rightNode != nil {
		node.rightHash = ndb.SaveBranch(node.rightNode)
	}

	obsoleteHash := node.hash
	node._hash()
	ndb.SaveNode(node)
	if obsoleteHash != nil {
		go ndb.uncacheNode(obsoleteHash)
	}

	node.leftNode = nil
	node.rightNode = nil

	return node.hash
}

// multi-threaded SaveBranch
// need to do 1. bfs first to launch, 2. post-order dfs to get hash
func (ndb *nodeDB) SaveBranchEx(node *Node, launchDepth int) ([]byte, int64, []tmdb.Batch) {
	var batches []tmdb.Batch
	var batchLock sync.Mutex

	type entry struct {
		depth int
		node  *Node
	}

	t := time.Now()
	m := int64(0)
	nt := 0

	// bfs for concurrent save branch
	if launchDepth > 0 {
		var wg sync.WaitGroup
		queue := list.New()
		curr := node
		depth := 0

		if !curr.persisted {
			queue.PushBack(&entry{depth: depth, node: curr})
		}
		for {
			if queue.Len() == 0 {
				break
			}
			e := queue.Front()
			curr, depth := e.Value.(*entry).node, e.Value.(*entry).depth
			queue.Remove(e)

			if depth == launchDepth-1 {
				if curr.leftNode != nil && !curr.leftNode.persisted {
					wg.Add(1)
					nt++
					go func(n *Node) {
						_, count, bs := ndb.SaveBranchEx(n, 0)
						batchLock.Lock()
						batches = append(batches, bs...)
						batchLock.Unlock()
						atomic.AddInt64(&m, count)
						wg.Done()
					}(curr.leftNode)
				}
				if curr.rightNode != nil && !curr.rightNode.persisted {
					wg.Add(1)
					nt++
					go func(n *Node) {
						_, count, bs := ndb.SaveBranchEx(n, 0)
						batchLock.Lock()
						batches = append(batches, bs...)
						batchLock.Unlock()
						atomic.AddInt64(&m, count)
						wg.Done()
					}(curr.rightNode)
				}
			} else {
				if curr.leftNode != nil && !curr.leftNode.persisted {
					queue.PushBack(&entry{depth: depth + 1, node: curr.leftNode})
				}
				if curr.rightNode != nil && !curr.rightNode.persisted {
					queue.PushBack(&entry{depth: depth + 1, node: curr.rightNode})
				}
			}
		}

		wg.Wait()
	}

	// post-order dfs
	batch := ndb.db.NewBatch()
	stack := list.New()
	curr := node
	depth := 0
	for {
		for curr != nil && !curr.persisted {
			if curr.rightNode != nil && !curr.rightNode.persisted {
				stack.PushBack(&entry{depth: depth + 1, node: curr.rightNode})
			}
			stack.PushBack(&entry{depth: depth, node: curr})
			curr = curr.leftNode
			depth++
		}

		if stack.Len() == 0 {
			break
		}

		e := stack.Back()
		curr, depth = e.Value.(*entry).node, e.Value.(*entry).depth
		stack.Remove(e)

		if curr.rightNode != nil && stack.Len() > 0 && stack.Back().Value.(*entry).node == curr.rightNode {
			stack.Remove(stack.Back())
			stack.PushBack(&entry{depth: depth, node: curr})
			curr = curr.rightNode
		} else {
			// visit this node

			if curr.leftNode != nil {
				curr.leftHash = curr.leftNode.hash
			}
			if curr.rightNode != nil {
				curr.rightHash = curr.rightNode.hash
			}

			obsoleteHash := curr.hash
			curr._hash()
			ndb.saveNodeEx(batch, curr)
			if obsoleteHash != nil {
				go ndb.uncacheNode(obsoleteHash)
			}

			curr.leftNode = nil
			curr.rightNode = nil
			curr = nil
			m++
		}
	}

	batches = append(batches, batch)

	dt := time.Since(t).Milliseconds()
	if launchDepth != 0 && dt > 500 {
		fmt.Printf("@@@ %s SaveBranchEx: visited=%d %d threads %d ms\n", ndb.db.Name(), m, nt, dt)
	}

	return node.hash, m, batches
}

// DeleteVersion deletes a tree version from disk.
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if ndb.versionReaders[version] > 0 {
		return errors.Errorf("unable to delete version %v, it has %v active readers", version, ndb.versionReaders[version])
	}

	ndb.deleteOrphans(version)
	ndb.deleteRoot(version, checkLatestVersion)
	return nil
}

// DeleteVersionsFrom permanently deletes all tree versions from the given version upwards.
func (ndb *nodeDB) DeleteVersionsFrom(version int64) error {
	latest := ndb.getLatestVersion()
	if latest < version {
		return nil
	}
	root, err := ndb.getRoot(latest)
	if err != nil {
		return err
	}
	if root == nil {
		return errors.Errorf("root for version %v not found", latest)
	}

	for v, r := range ndb.versionReaders {
		if v >= version && r != 0 {
			return errors.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// First, delete all active nodes in the current (latest) version whose node version is after
	// the given version.
	err = ndb.deleteNodesFrom(version, root)
	if err != nil {
		return err
	}

	// Next, delete orphans:
	// - Delete orphan entries *and referred nodes* with fromVersion >= version
	// - Delete orphan entries with toVersion >= version-1 (since orphans at latest are not orphans)
	ndb.traverseOrphans(func(key, hash []byte) {
		var fromVersion, toVersion int64
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		if fromVersion >= version {
			if err = ndb.batch.Delete(key); err != nil {
				panic(err)
			}
			if err = ndb.batch.Delete(ndb.nodeKey(hash)); err != nil {
				panic(err)
			}
			ndb.uncacheNode(hash)
		} else if toVersion >= version-1 {
			if err := ndb.batch.Delete(key); err != nil {
				panic(err)
			}
		}
	})

	// Finally, delete the version root entries
	ndb.traverseRange(rootKeyFormat.Key(version), rootKeyFormat.Key(int64(math.MaxInt64)), func(k, v []byte) {
		if err := ndb.batch.Delete(k); err != nil {
			panic(err)
		}
	})

	return nil
}

// DeleteVersionsRange deletes versions from an interval (not inclusive).
func (ndb *nodeDB) DeleteVersionsRange(fromVersion, toVersion int64) error {
	if fromVersion >= toVersion {
		return errors.New("toVersion must be greater than fromVersion")
	}
	if toVersion == 0 {
		return errors.New("toVersion must be greater than 0")
	}

	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	latest := ndb.getLatestVersion()
	if latest < toVersion {
		return errors.Errorf("cannot delete latest saved version (%d)", latest)
	}

	predecessor := ndb.getPreviousVersion(fromVersion)

	for v, r := range ndb.versionReaders {
		if v < toVersion && v > predecessor && r != 0 {
			return errors.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// If the predecessor is earlier than the beginning of the lifetime, we can delete the orphan.
	// Otherwise, we shorten its lifetime, by moving its endpoint to the predecessor version.
	for version := fromVersion; version < toVersion; version++ {
		ndb.traverseOrphansVersion(version, func(key, hash []byte) {
			var from, to int64
			orphanKeyFormat.Scan(key, &to, &from)
			if err := ndb.batch.Delete(key); err != nil {
				panic(err)
			}
			if from > predecessor {
				if err := ndb.batch.Delete(ndb.nodeKey(hash)); err != nil {
					panic(err)
				}
				ndb.uncacheNode(hash)
			} else {
				ndb.saveOrphan(hash, from, predecessor)
			}
		})
	}

	// Delete the version root entries
	ndb.traverseRange(rootKeyFormat.Key(fromVersion), rootKeyFormat.Key(toVersion), func(k, v []byte) {
		if err := ndb.batch.Delete(k); err != nil {
			panic(err)
		}
	})

	return nil
}

// DeleteVersionsRange deletes versions from an interval (not inclusive).
func (ndb *nodeDB) DeleteVersionsRangeEx(fromVersion, toVersion int64) (int, error) {
	if fromVersion >= toVersion {
		return 0, errors.New("toVersion must be greater than fromVersion")
	}
	if toVersion == 0 {
		return 0, errors.New("toVersion must be greater than 0")
	}

	ndb.mtx.Lock()

	latest := ndb.getLatestVersion()
	if latest < toVersion {
		ndb.mtx.Unlock()
		return 0, errors.Errorf("cannot delete latest saved version (%d)", latest)
	}

	predecessor := ndb.getPreviousVersion(fromVersion)

	for v, r := range ndb.versionReaders {
		if v < toVersion && v > predecessor && r != 0 {
			ndb.mtx.Unlock()
			return 0, errors.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}
	ndb.mtx.Unlock()

	// If the predecessor is earlier than the beginning of the lifetime, we can delete the orphan.
	// Otherwise, we shorten its lifetime, by moving its endpoint to the predecessor version.
	batch := ndb.db.NewBatch()
	count, total, max := 0, 0, PruningBatchSize
	for version := fromVersion; version < toVersion; version++ {
		ndb.traverseOrphansVersion(version, func(key, hash []byte) {
			var from, to int64
			orphanKeyFormat.Scan(key, &to, &from)
			if err := batch.Delete(key); err != nil {
				panic(err)
			}
			if from > predecessor {
				if err := batch.Delete(ndb.nodeKey(hash)); err != nil {
					panic(err)
				}
				ndb.uncacheNode(hash)
			} else {
				ndb.saveOrphanEx(batch, hash, from, predecessor)
			}
			count++
			total++
			if count >= max {
				if err := ndb.commitExLowPri(batch); err != nil {
					panic(err)
				}
				batch = ndb.db.NewBatch()
				count = 0
			}
		})
	}

	// Delete the version root entries
	ndb.traverseRange(rootKeyFormat.Key(fromVersion), rootKeyFormat.Key(toVersion), func(k, v []byte) {
		if err := batch.Delete(k); err != nil {
			panic(err)
		}
	})
	if err := ndb.commitExLowPri(batch); err != nil {
		panic(err)
	}

	return total, nil
}

// deleteNodesFrom deletes the given node and any descendants that have versions after the given
// (inclusive). It is mainly used via LoadVersionForOverwriting, to delete the current version.
func (ndb *nodeDB) deleteNodesFrom(version int64, hash []byte) error {
	if len(hash) == 0 {
		return nil
	}

	node := ndb.GetNode(hash)
	if node.leftHash != nil {
		if err := ndb.deleteNodesFrom(version, node.leftHash); err != nil {
			return err
		}
	}
	if node.rightHash != nil {
		if err := ndb.deleteNodesFrom(version, node.rightHash); err != nil {
			return err
		}
	}

	if node.version >= version {
		if err := ndb.batch.Delete(ndb.nodeKey(hash)); err != nil {
			return err
		}

		ndb.uncacheNode(hash)
	}

	return nil
}

// Saves orphaned nodes to disk under a special prefix.
// version: the new version being saved.
// orphans: the orphan nodes created since version-1
func (ndb *nodeDB) SaveOrphans(version int64, orphans map[string]int64) {
	ndb.mtx.Lock()
	toVersion := ndb.getPreviousVersion(version)
	ndb.mtx.Unlock()
	for hash, fromVersion := range orphans {
		debug("SAVEORPHAN %v-%v %X\n", fromVersion, toVersion, hash)
		ndb.saveOrphan([]byte(hash), fromVersion, toVersion)
	}
}

// Saves a single orphan to disk.
func (ndb *nodeDB) saveOrphan(hash []byte, fromVersion, toVersion int64) {
	if fromVersion > toVersion {
		panic(fmt.Sprintf("Orphan expires before it comes alive.  %d > %d", fromVersion, toVersion))
	}
	key := ndb.orphanKey(fromVersion, toVersion, hash)
	if err := ndb.batch.Set(key, hash); err != nil {
		panic(err)
	}
}

func (ndb *nodeDB) saveOrphansEx(version int64, orphans map[string]int64) tmdb.Batch {
	ndb.mtx.Lock()
	toVersion := ndb.getPreviousVersion(version)
	ndb.mtx.Unlock()

	batch := ndb.db.NewBatch()
	for hash, fromVersion := range orphans {
		debug("SAVEORPHAN %v-%v %X\n", fromVersion, toVersion, hash)
		ndb.saveOrphanEx(batch, []byte(hash), fromVersion, toVersion)
	}
	return batch
}

func (ndb *nodeDB) saveOrphanEx(batch tmdb.Batch, hash []byte, fromVersion, toVersion int64) {
	if fromVersion > toVersion {
		panic(fmt.Sprintf("Orphan expires before it comes alive.  %d > %d", fromVersion, toVersion))
	}
	key := ndb.orphanKey(fromVersion, toVersion, hash)
	if err := batch.Set(key, hash); err != nil {
		panic(err)
	}
}

// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64) {
	// Will be zero if there is no previous version.
	predecessor := ndb.getPreviousVersion(version)

	// Traverse orphans with a lifetime ending at the version specified.
	// TODO optimize.
	ndb.traverseOrphansVersion(version, func(key, hash []byte) {
		var fromVersion, toVersion int64

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		// Delete orphan key and reverse-lookup key.
		if err := ndb.batch.Delete(key); err != nil {
			panic(err)
		}

		// If there is no predecessor, or the predecessor is earlier than the
		// beginning of the lifetime (ie: negative lifetime), or the lifetime
		// spans a single version and that version is the one being deleted, we
		// can delete the orphan.  Otherwise, we shorten its lifetime, by
		// moving its endpoint to the previous version.
		if predecessor < fromVersion || fromVersion == toVersion {
			debug("DELETE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
			if err := ndb.batch.Delete(ndb.nodeKey(hash)); err != nil {
				panic(err)
			}
			ndb.uncacheNode(hash)
		} else {
			debug("MOVE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
			ndb.saveOrphan(hash, fromVersion, predecessor)
		}
	})
}

func (ndb *nodeDB) nodeKey(hash []byte) []byte {
	return nodeKeyFormat.KeyBytes(hash)
}

func (ndb *nodeDB) orphanKey(fromVersion, toVersion int64, hash []byte) []byte {
	return orphanKeyFormat.Key(toVersion, fromVersion, hash)
}

func (ndb *nodeDB) rootKey(version int64) []byte {
	return rootKeyFormat.Key(version)
}

func (ndb *nodeDB) getLatestVersion() int64 {
	if ndb.latestVersion == 0 {
		ndb.latestVersion = ndb.getPreviousVersion(1<<63 - 1)
	}
	return ndb.latestVersion
}

func (ndb *nodeDB) updateLatestVersion(version int64) {
	if ndb.latestVersion < version {
		ndb.latestVersion = version
	}
}

func (ndb *nodeDB) resetLatestVersion(version int64) {
	ndb.latestVersion = version
}

func (ndb *nodeDB) getPreviousVersion(version int64) int64 {
	itr, err := ndb.db.ReverseIterator(
		rootKeyFormat.Key(1),
		rootKeyFormat.Key(version),
	)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	pversion := int64(-1)
	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		rootKeyFormat.Scan(k, &pversion)
		return pversion
	}

	if err := itr.Error(); err != nil {
		panic(err)
	}

	return 0
}

// deleteRoot deletes the root entry from disk, but not the node it points to.
func (ndb *nodeDB) deleteRoot(version int64, checkLatestVersion bool) {
	if checkLatestVersion && version == ndb.getLatestVersion() {
		panic("Tried to delete latest version")
	}
	if err := ndb.batch.Delete(ndb.rootKey(version)); err != nil {
		panic(err)
	}
}

func (ndb *nodeDB) traverseOrphans(fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(), fn)
}

// Traverse orphans ending at a certain version.
func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(version), fn)
}

// Traverse all keys.
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	ndb.traverseRange(nil, nil, fn)
}

// Traverse all keys between a given range (excluding end).
func (ndb *nodeDB) traverseRange(start []byte, end []byte, fn func(k, v []byte)) {
	itr, err := ndb.db.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}

	if err := itr.Error(); err != nil {
		panic(err)
	}
}

// Traverse all keys with a certain prefix.
func (ndb *nodeDB) traversePrefix(prefix []byte, fn func(k, v []byte)) {
	itr, err := ndb.db.PrefixIterator(prefix)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

func (ndb *nodeDB) uncacheNode(hash []byte) {
	if ndb.nodeCache != nil {
		ndb.nodeCache.Del(hash)
	}
}

// Add a node to the cache and pop the least recently used node if we've
// reached the cache size limit.
func (ndb *nodeDB) cacheNode(key, nodeBytes []byte) {
	if ndb.nodeCache != nil {
		ndb.nodeCache.Set(key, nodeBytes)
		go func() {
			if ndb.kind == 0 {
				name := ndb.db.Name()
				if strings.Contains(name, "acc") {
					ndb.kind = 1
				} else if strings.Contains(name, "bank") {
					ndb.kind = 2
				} else if strings.Contains(name, "staking") {
					ndb.kind = 3
				} else {
					ndb.kind = 4
				}
			}
			switch ndb.kind {
			case 1: // acc
				atomic.AddInt64(&accCount, 1)
				atomic.AddInt64(&accSize, int64(len(nodeBytes)))
			case 2: // bank
				atomic.AddInt64(&bankCount, 1)
				atomic.AddInt64(&bankSize, int64(len(nodeBytes)))
			case 3: // staking
				atomic.AddInt64(&stakingCount, 1)
				atomic.AddInt64(&stakingSize, int64(len(nodeBytes)))
			}
			atomic.AddInt64(&totalCount, 1)
			atomic.AddInt64(&totalSize, int64(len(nodeBytes)))
		}()
	}
}

// Write to disk.
func (ndb *nodeDB) Commit() error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	var err error
	if ndb.opts.Sync {
		err = ndb.batch.WriteSync()
	} else {
		err = ndb.batch.Write()
	}
	if err != nil {
		return errors.Wrap(err, "failed to write batch")
	}

	ndb.batch.Close()
	ndb.batch = ndb.db.NewBatch()

	return nil
}

func (ndb *nodeDB) commitEx(batches ...tmdb.Batch) error {
	var err error
	var wg sync.WaitGroup
	for _, batch := range batches {
		wg.Add(1)
		go func(b tmdb.Batch) {
			var e error
			if ndb.opts.Sync {
				e = b.WriteSync()
			} else {
				e = b.Write()
			}
			if err == nil && e != nil {
				err = e
			}
			wg.Done()
		}(batch)
	}
	wg.Wait()
	return err
}

func (ndb *nodeDB) commitExLowPri(batches ...tmdb.Batch) error {
	var err error
	var wg sync.WaitGroup
	for _, batch := range batches {
		wg.Add(1)
		go func(b tmdb.Batch) {
			if e := b.WriteLowPri(); e != nil && err == nil {
				err = e
			}
			wg.Done()
		}(batch)
	}
	wg.Wait()
	return err
}

func (ndb *nodeDB) getRoot(version int64) ([]byte, error) {
	return ndb.db.Get(ndb.rootKey(version))
}

func (ndb *nodeDB) getRoots() (map[int64][]byte, error) {
	roots := map[int64][]byte{}

	ndb.traversePrefix(rootKeyFormat.Key(), func(k, v []byte) {
		var version int64
		rootKeyFormat.Scan(k, &version)
		roots[version] = v
	})
	return roots, nil
}

// SaveRoot creates an entry on disk for the given root, so that it can be
// loaded later.
func (ndb *nodeDB) SaveRoot(root *Node, version int64) error {
	if len(root.hash) == 0 {
		panic("SaveRoot: root hash should not be empty")
	}
	return ndb.saveRoot(root.hash, version)
}

// SaveEmptyRoot creates an entry on disk for an empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64) error {
	return ndb.saveRoot([]byte{}, version)
}

func (ndb *nodeDB) saveRoot(hash []byte, version int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	// We allow the initial version to be arbitrary
	latest := ndb.getLatestVersion()
	if latest > 0 && version != latest+1 {
		return fmt.Errorf("must save consecutive versions; expected %d, got %d", latest+1, version)
	}

	if err := ndb.batch.Set(ndb.rootKey(version), hash); err != nil {
		return err
	}

	ndb.updateLatestVersion(version)

	return nil
}

func (ndb *nodeDB) saveRootEx(batch tmdb.Batch, hash []byte, version int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	// We allow the initial version to be arbitrary
	latest := ndb.getLatestVersion()
	if latest > 0 && version != latest+1 {
		return fmt.Errorf("must save consecutive versions; expected %d, got %d", latest+1, version)
	}

	if err := batch.Set(ndb.rootKey(version), hash); err != nil {
		return err
	}

	ndb.updateLatestVersion(version)

	return nil
}

func (ndb *nodeDB) incrVersionReaders(version int64) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	ndb.versionReaders[version]++
}

func (ndb *nodeDB) decrVersionReaders(version int64) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if ndb.versionReaders[version] > 0 {
		ndb.versionReaders[version]--
	}
}

// Utility and test functions

func (ndb *nodeDB) leafNodes() []*Node {
	leaves := []*Node{}

	ndb.traverseNodes(func(hash []byte, node *Node) {
		if node.isLeaf() {
			leaves = append(leaves, node)
		}
	})
	return leaves
}

func (ndb *nodeDB) nodes() []*Node {
	nodes := []*Node{}

	ndb.traverseNodes(func(hash []byte, node *Node) {
		nodes = append(nodes, node)
	})
	return nodes
}

func (ndb *nodeDB) orphans() [][]byte {
	orphans := [][]byte{}

	ndb.traverseOrphans(func(k, v []byte) {
		orphans = append(orphans, v)
	})
	return orphans
}

func (ndb *nodeDB) roots() map[int64][]byte {
	roots, _ := ndb.getRoots()
	return roots
}

// Not efficient.
// NOTE: DB cannot implement Size() because
// mutations are not always synchronous.
func (ndb *nodeDB) size() int {
	size := 0
	ndb.traverse(func(k, v []byte) {
		size++
	})
	return size
}

func (ndb *nodeDB) traverseNodes(fn func(hash []byte, node *Node)) {
	nodes := []*Node{}

	ndb.traversePrefix(nodeKeyFormat.Key(), func(key, value []byte) {
		node, err := MakeNode(value)
		if err != nil {
			panic(fmt.Sprintf("Couldn't decode node from database: %v", err))
		}
		nodeKeyFormat.Scan(key, &node.hash)
		nodes = append(nodes, node)
	})

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].key, nodes[j].key) < 0
	})

	for _, n := range nodes {
		fn(n.hash, n)
	}
}

func (ndb *nodeDB) String() string {
	var str string
	index := 0

	ndb.traversePrefix(rootKeyFormat.Key(), func(key, value []byte) {
		str += fmt.Sprintf("%s: %x\n", string(key), value)
	})
	str += "\n"

	ndb.traverseOrphans(func(key, value []byte) {
		str += fmt.Sprintf("%s: %x\n", string(key), value)
	})
	str += "\n"

	ndb.traverseNodes(func(hash []byte, node *Node) {
		switch {
		case len(hash) == 0:
			str += "<nil>\n"
		case node == nil:
			str += fmt.Sprintf("%s%40x: <nil>\n", nodeKeyFormat.Prefix(), hash)
		case node.value == nil && node.height > 0:
			str += fmt.Sprintf("%s%40x: %s   %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, "", node.height, node.version)
		default:
			str += fmt.Sprintf("%s%40x: %s = %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, node.value, node.height, node.version)
		}
		index++
	})
	return "-" + "\n" + str + "-"
}
