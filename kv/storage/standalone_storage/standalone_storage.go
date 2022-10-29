package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

var _ storage.Storage = &StandAloneStorage{}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) (*StandAloneStorage, error) {
	opt := badger.DefaultOptions
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath

	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &StandAloneStorage{db: db}, nil
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	return StandAloneStorageReader{
		db: s.db,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	t := s.db.NewTransaction(true)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := t.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := t.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			if err != nil {
				return err
			}
		}

	}
	return t.Commit()
}

type StandAloneStorageReader struct {
	db *badger.DB
}

var ErrNotFound = errors.New("not found")

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		v, err := txn.Get(engine_util.KeyWithCF(cf, key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			value = nil
			return ErrNotFound
		}
		value, err = v.Value()
		if err != nil {
			return err
		}

		return nil
	})

	return value, err
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := s.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (s StandAloneStorageReader) Close() {
}

type StandAloneStorageIterator struct {
	it     *badger.Iterator
	prefix string
}
