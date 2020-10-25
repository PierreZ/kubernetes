package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

type watcher struct {
	db 			fdb.Database
	codec       runtime.Codec
	newFunc     func() runtime.Object
	versioner   storage.Versioner
	transformer value.Transformer
}

func (w watcher) Watch(ctx context.Context, key string, revision int64, recursive bool, progressNotify bool, predicate storage.SelectionPredicate) (watch.Interface, error) {
	return nil, nil
}

func newWatcher(db fdb.Database, codec runtime.Codec, newFunc func() runtime.Object, versioner storage.Versioner, transformer value.Transformer) *watcher {
	return &watcher{
		db, codec, newFunc,versioner, transformer,
	}
}