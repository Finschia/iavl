package iavl

import (
	"sync"

	tmdb "github.com/line/tm-db/v2"
)

var DefaultBatchSize int = 5000

type Batch struct {
	db        tmdb.DB
	batchSize int
	count     int
	curr      tmdb.Batch
	batches   []tmdb.Batch
}

func NewBatch(db tmdb.DB, batchSize int) tmdb.Batch {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &Batch{
		db:        db,
		batchSize: batchSize,
		count:     0,
		curr:      db.NewBatch(),
	}
}

func (b *Batch) prep() {
	n := b.count % b.batchSize
	if b.count != 0 && n == 0 {
		// need a new batch
		if b.curr != nil {
			b.batches = append(b.batches, b.curr)
		}
		b.curr = b.db.NewBatch()
	}
}

func (b *Batch) Set(key, value []byte) error {
	b.prep()
	err := b.curr.Set(key, value)
	if err == nil {
		b.count += 1
	}
	return err
}

func (b *Batch) Delete(key []byte) error {
	b.prep()
	err := b.curr.Delete(key)
	if err == nil {
		b.count += 1
	}
	return err
}

func (b *Batch) Write() error {
	if b.count <= 0 {
		return nil
	}

	var wg sync.WaitGroup
	var err error

	write := func(bb tmdb.Batch) {
		errr := bb.Write()
		if errr != nil {
			err = errr
		}
		wg.Done()
	}

	for _, b := range b.batches {
		wg.Add(1)
		go write(b)
	}
	wg.Add(1)
	go write(b.curr)

	wg.Wait()

	return err
}

func (b *Batch) WriteSync() error {
	return b.Write()
}

func (b *Batch) Close() error {
	for _, bb := range b.batches {
		bb.Close()
	}
	if b.curr != nil {
		b.curr.Close()
	}
	b.batches = nil
	return nil
}
