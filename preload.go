package iavl

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// preload warms the cache by visiting and loading tree nodes of the latest.
// It launches goroutines at 'launchDepth' level, so (2 ^ 'launchDepth' + 1)
// goroutines will run together. E.g. 129 goroutines for launchDepth 7.
// It does breadth-first traversal up to launchDepth level, after which
// depth-first traversal to not waste memory.
// It validates the latest tree as a side-effect.
func (ndb *nodeDB) preload(hash []byte, launchDepth, maxDepth int) int64 {
	var latest int64

	// to capture panic when the tree is broken, most likelye when node doesn't exist.
	// It's going to panic later anyways.
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("@@@ %s preloading %d -> getnode panic'ed: %v\n", ndb.db.Name(), latest, msg)
		}
	}()

	type entry struct {
		depth int
		hash  []byte
	}

	var wg sync.WaitGroup
	t := time.Now()
	l := list.New()
	m := int64(0)
	nt := 0

	if len(hash) == 0 {
		ndb.mtx.Lock()
		latest = ndb.getLatestVersion()
		ndb.mtx.Unlock()
		h, err := ndb.getRoot(latest)
		if err != nil || len(h) == 0 {
			return 0
		}
		hash = h
	}
	l.PushBack(&entry{depth: 0, hash: hash})

	for {
		if l.Len() == 0 {
			break
		}
		atomic.AddInt64(&m, 1)

		var h []byte
		var d int
		if launchDepth == 0 {
			// depth first
			e := l.Back()
			d, h = e.Value.(*entry).depth, e.Value.(*entry).hash
			l.Remove(e)
		} else {
			// breadth first
			e := l.Front()
			d, h = e.Value.(*entry).depth, e.Value.(*entry).hash
			l.Remove(e)
		}

		n := func(h []byte) *Node {
			defer func() {
				if msg := recover(); msg != nil {
					fmt.Printf("@@@ %s preloading %d: %v\n", ndb.db.Name(), latest, msg)
				}
			}()
			node := ndb.GetNode(h)
			return node
		}(h)
		if n == nil || (maxDepth > 0 && d+1 > maxDepth) {
			continue
		}
		if launchDepth != 0 && d == launchDepth-1 {
			if len(n.leftHash) > 0 {
				nt++
				wg.Add(1)
				go func() {
					n := ndb.preload(n.leftHash, 0, maxDepth)
					atomic.AddInt64(&m, n)
					wg.Done()
				}()
			}
			if len(n.rightHash) > 0 {
				nt++
				wg.Add(1)
				go func() {
					n := ndb.preload(n.rightHash, 0, maxDepth)
					atomic.AddInt64(&m, n)
					wg.Done()
				}()
			}
		} else {
			if len(n.leftHash) > 0 {
				l.PushBack(&entry{depth: d + 1, hash: n.leftHash})
			}
			if len(n.rightHash) > 0 {
				l.PushBack(&entry{depth: d + 1, hash: n.rightHash})
			}
		}
	}
	wg.Wait()

	dt := time.Since(t).Milliseconds()
	if launchDepth != 0 && dt > 1 {
		fmt.Printf("@@@ %s height=%d visited=%d %d threads %d ms\n", ndb.db.Name(), latest, m, nt, dt)
	}
	return m
}
