package factory

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/fdb"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value"
)

func newFDBStorage(c storagebackend.Config, newFunc func() runtime.Object) (storage.Interface, DestroyFunc, error) {
	transformer := c.Transformer
	if transformer == nil {
		transformer = value.IdentityTransformer
	}

	store, err := fdb.New(c.Codec, transformer, newFunc)
	if err != nil {
		return nil, nil, err
	}
	return store, nil, nil
}
