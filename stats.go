package iavl

import (
	"bytes"
)

// cache hit ratio
type Stats struct {
	// hits / misses including prefetch hits / misses
	Hits   int64
	Misses int64
	// prefetch hits / misses
	PrefetchHits   int64
	PrefetchMisses int64
	// hits / misses during set operation
	SetHits   int64
	SetMisses int64
	// sets / news / rotates
	Sets    int64
	News    int64
	Rotates int64
}

var (
	statsEnabled = false
	stats        Stats
)

func GetStats() Stats {
	// TODO: if not atomic, should use atomic package
	return stats
}

func ResetStats() {
	stats.Hits, stats.Misses = 0, 0
	stats.PrefetchHits, stats.PrefetchMisses = 0, 0
	stats.SetHits, stats.SetMisses = 0, 0
	stats.Sets, stats.News, stats.Rotates = 0, 0, 0
}

func EnableStats() {
	statsEnabled = true
}

func DisableStats() {
	statsEnabled = true
}

// for set with stats
func (node *Node) calcHeightAndSizeEx(t *ImmutableTree) (hits, misses int) {
	left, hit := node.getLeftNodeEx(t)
	if hit {
		hits++
	} else {
		misses++
	}
	right, hit := node.getRightNodeEx(t)
	if hit {
		hits++
	} else {
		misses++
	}
	node.height = maxInt8(left.height, right.height) + 1
	node.size = left.size + right.size
	return hits, misses
}

func (node *Node) calcBalanceEx(t *ImmutableTree) (balance, hits, misses int) {
	left, hit := node.getLeftNodeEx(t)
	if hit {
		hits++
	} else {
		misses++
	}
	right, hit := node.getRightNodeEx(t)
	if hit {
		hits++
	} else {
		misses++
	}
	return int(left.height) - int(right.height), hits, misses
}

func (tree *MutableTree) recursiveSetEx(node *Node, key []byte, value []byte, orphans *[]*Node) (
	newSelf *Node, updated bool, hits, misses, rotates int,
) {
	version := tree.version + 1

	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			return &Node{
				key:       node.key,
				height:    1,
				size:      2,
				leftNode:  NewNode(key, value, version),
				rightNode: node,
				version:   version,
			}, false, 0, 0, 0
		case 1:
			return &Node{
				key:       key,
				height:    1,
				size:      2,
				leftNode:  node,
				rightNode: NewNode(key, value, version),
				version:   version,
			}, false, 0, 0, 0
		default:
			*orphans = append(*orphans, node)
			return NewNode(key, value, version), true, 0, 0, 0
		}
	} else {
		*orphans = append(*orphans, node)
		node = node.clone(version)

		if bytes.Compare(key, node.key) < 0 {
			left, hit := node.getLeftNodeEx(tree.ImmutableTree)
			if hit {
				hits++
			} else {
				misses++
			}
			var ihits, imisses, irotates int
			node.leftNode, updated, ihits, imisses, irotates = tree.recursiveSetEx(left, key, value, orphans)
			node.leftHash = nil // leftHash is yet unknown
			hits += ihits
			misses += imisses
			rotates += irotates
		} else {
			right, hit := node.getRightNodeEx(tree.ImmutableTree)
			if hit {
				hits++
			} else {
				misses++
			}
			var ihits, imisses, irotates int
			node.rightNode, updated, ihits, imisses, irotates = tree.recursiveSetEx(right, key, value, orphans)
			node.rightHash = nil // rightHash is yet unknown
			hits += ihits
			misses += imisses
			rotates += irotates
		}

		if updated {
			return node, updated, hits, misses, rotates
		}
		ihits, imisses := node.calcHeightAndSizeEx(tree.ImmutableTree)
		hits += ihits
		misses += imisses
		newNode, ihits, imisses, irotates := tree.balanceEx(node, orphans)
		hits += ihits
		misses += imisses
		rotates += irotates
		return newNode, updated, hits, misses, rotates
	}
}

func (node *Node) getLeftNodeEx(t *ImmutableTree) (leftNode *Node, cacheHit bool) {
	if node.leftNode != nil {
		return node.leftNode, true
	}
	return t.ndb.GetNodeEx(node.leftHash)
}

func (node *Node) getRightNodeEx(t *ImmutableTree) (rightNode *Node, cacheHit bool) {
	if node.rightNode != nil {
		return node.rightNode, true
	}
	return t.ndb.GetNodeEx(node.rightHash)
}

// Rotate right and return the new node and orphan.
func (tree *MutableTree) rotateRightEx(node *Node) (*Node, *Node, int, int) {
	hits, misses := 0, 0
	version := tree.version + 1

	// TODO: optimize balance & rotate.
	node = node.clone(version)
	orphaned, hit := node.getLeftNodeEx(tree.ImmutableTree)
	if hit {
		hits++
	} else {
		misses++
	}
	newNode := orphaned.clone(version)

	newNoderHash, newNoderCached := newNode.rightHash, newNode.rightNode
	newNode.rightHash, newNode.rightNode = node.hash, node
	node.leftHash, node.leftNode = newNoderHash, newNoderCached

	ihits, imisses := node.calcHeightAndSizeEx(tree.ImmutableTree)
	hits += ihits
	misses += imisses
	ihits, imisses = newNode.calcHeightAndSizeEx(tree.ImmutableTree)
	hits += ihits
	misses += imisses
	return newNode, orphaned, hits, misses
}

// Rotate left and return the new node and orphan.
func (tree *MutableTree) rotateLeftEx(node *Node) (*Node, *Node, int, int) {
	hits, misses := 0, 0
	version := tree.version + 1

	// TODO: optimize balance & rotate.
	node = node.clone(version)
	orphaned, hit := node.getRightNodeEx(tree.ImmutableTree)
	if hit {
		hits++
	} else {
		misses++
	}
	newNode := orphaned.clone(version)

	newNodelHash, newNodelCached := newNode.leftHash, newNode.leftNode
	newNode.leftHash, newNode.leftNode = node.hash, node
	node.rightHash, node.rightNode = newNodelHash, newNodelCached

	ihits, imisses := node.calcHeightAndSizeEx(tree.ImmutableTree)
	hits += ihits
	misses += imisses
	ihits, imisses = newNode.calcHeightAndSizeEx(tree.ImmutableTree)
	hits += ihits
	misses += imisses

	return newNode, orphaned, hits, misses
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (tree *MutableTree) balanceEx(node *Node, orphans *[]*Node) (newSelf *Node, hits, misses, rotates int) {
	if node.persisted {
		panic("Unexpected balance() call on persisted node")
	}
	balance, ihits, imisses := node.calcBalanceEx(tree.ImmutableTree)
	hits += ihits
	misses += imisses

	if balance > 1 { //nolint
		left, hit := node.getLeftNodeEx(tree.ImmutableTree)
		if hit {
			hits++
		} else {
			misses++
		}
		ibal, ihits, imisses := left.calcBalanceEx(tree.ImmutableTree)
		hits += ihits
		misses += imisses
		if ibal >= 0 {
			// Left Left Case
			newNode, orphaned, ihits2, imisses2 := tree.rotateRightEx(node)
			rotates++
			hits += ihits2
			misses += imisses2
			*orphans = append(*orphans, orphaned)
			return newNode, hits, misses, rotates
		}
		// Left Right Case
		var leftOrphaned *Node

		left, hit = node.getLeftNodeEx(tree.ImmutableTree)
		if hit {
			hits++
		} else {
			misses++
		}
		node.leftHash = nil
		node.leftNode, leftOrphaned, ihits, imisses = tree.rotateLeftEx(left)
		rotates++
		hits += ihits
		misses += imisses
		newNode, rightOrphaned, ihits, imisses := tree.rotateRightEx(node)
		rotates++
		hits += ihits
		misses += imisses
		*orphans = append(*orphans, left, leftOrphaned, rightOrphaned)
		return newNode, hits, misses, rotates
	}
	if balance < -1 { //nolint
		right, hit := node.getRightNodeEx(tree.ImmutableTree)
		if hit {
			hits++
		} else {
			misses++
		}
		ibal, ihits, imisses := right.calcBalanceEx(tree.ImmutableTree)
		hits += ihits
		misses += imisses
		if ibal <= 0 {
			// Right Right Case
			newNode, orphaned, ihits2, imisses2 := tree.rotateLeftEx(node)
			rotates++
			hits += ihits2
			misses += imisses2
			*orphans = append(*orphans, orphaned)
			return newNode, hits, misses, rotates
		}
		// Right Left Case
		var rightOrphaned *Node

		right, hit = node.getRightNodeEx(tree.ImmutableTree)
		if hit {
			hits++
		} else {
			misses++
		}
		node.rightHash = nil
		node.rightNode, rightOrphaned, ihits, imisses = tree.rotateRightEx(right)
		rotates++
		hits += ihits
		misses += imisses
		newNode, leftOrphaned, ihits, imisses := tree.rotateLeftEx(node)
		rotates++
		hits += ihits
		misses += imisses

		*orphans = append(*orphans, right, leftOrphaned, rightOrphaned)
		return newNode, hits, misses, rotates
	}
	// Nothing changed
	return node, hits, misses, rotates
}

// EOF
