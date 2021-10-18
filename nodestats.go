package iavl

var (
	// updated node stats
	nodeSetCount uint64
	nodeSetBytes uint64
	nodeDelCount uint64
)

// returns updated node stats
func GetDBStats() (setCount, setBytes, delCount uint64) {
	setCount, setBytes, delCount = nodeSetCount, nodeSetBytes, nodeDelCount
	return
}
