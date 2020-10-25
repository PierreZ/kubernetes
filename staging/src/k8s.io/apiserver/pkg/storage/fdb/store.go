package fdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
	"path"
	"reflect"
)

type authenticatedDataString string

// AuthenticatedData implements the value.Context interface.
func (d authenticatedDataString) AuthenticatedData() []byte {
	return []byte(string(d))
}

var _ value.Context = authenticatedDataString("")

type Store struct {
	client     fdb.Database
	codec      runtime.Codec
	pathPrefix string
	versioner  storage.Versioner
	directory  directory.DirectorySubspace
	transformer   value.Transformer
	keySpace   subspace.Subspace
	watcher *watcher
}

func New(codec runtime.Codec, transformer value.Transformer, newFunc func() runtime.Object) (storage.Interface, error) {
	versioner := APIObjectVersioner{}
	fdb.MustAPIVersion(620)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	dir, err := directory.CreateOrOpen(db, []string{"k8s"}, nil)
	if err != nil {
		return nil, err
	}
	sub := dir.Sub("key-prefix1")
	return &Store{
		codec:       codec,
		directory:   dir,
		keySpace:    sub,
		versioner:   etcd3.APIObjectVersioner{},
		transformer: transformer,
		client:      db,
		watcher:      newWatcher(db, codec, newFunc, versioner, transformer),
	}, nil
}

func (s Store) Versioner() storage.Versioner {
	return s.versioner
}

func (s Store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion should not be set on objects to be created")
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	data, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return err
	}

	fdbKey := tuple.Tuple{key}
	_, err = s.client.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if !s.keySpace.Contains(fdbKey) {
			tr.Set(fdbKey, data)
		}
		return nil, nil
	})
	return err
}

func (s Store) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc) error {
	_, err := s.client.Transact(func(tr fdb.Transaction) (interface{}, error) {
		fdbKey := s.keySpace.Pack(tuple.Tuple{key})
		tr.Clear(fdbKey)
		return nil, nil
	})
	return err
}
// Watch implements storage.Interface.Watch.
func (s *Store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.watch(ctx, key, opts, false)
}

// WatchList implements storage.Interface.WatchList.
func (s *Store) WatchList(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.watch(ctx, key, opts, true)
}

func (s Store) watch(ctx context.Context, key string, opts storage.ListOptions, recursive bool) (watch.Interface, error) {
	rev, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	key = path.Join(s.pathPrefix, key)
	return s.watcher.Watch(ctx, key, int64(rev), recursive, opts.ProgressNotify, opts.Predicate)
}


func (s Store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	var readVersion int64
	resultInterface, err := s.client.Transact(func(tr fdb.Transaction) (interface{}, error) {
		var e error
		readVersion, e = tr.GetReadVersion().Get()
		if e != nil {
			return nil, e
		}
		fdbKey := s.keySpace.Pack(tuple.Tuple{key})
		return tr.Get(fdbKey).Get()
	})
	if err != nil {
		return err
	}
	v := resultInterface.([]byte)
	data, _, err := s.transformer.TransformFromStorage(v, authenticatedDataString(key))
	if err != nil {
		return storage.NewInternalError(err.Error())
	}
	return decode(s.codec, s.versioner, data, out, readVersion)
}

// decode decodes value of bytes into object. It will also set the object resource version to rev.
// On success, objPtr would be set to the object.
func decode(codec runtime.Codec, versioner storage.Versioner, value []byte, objPtr runtime.Object, rev int64) error {
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_, _, err := codec.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	if err := versioner.UpdateObject(objPtr, uint64(rev)); err != nil {
		klog.Errorf("failed to update object version: %v", err)
	}
	return nil
}

func (s Store) GetToList(ctx context.Context, key string, listOpts storage.ListOptions, listObj runtime.Object) error {
	pred := listOpts.Predicate

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}
	newItemFunc := getNewItemFunc(listObj, v)

	keyRange, err := fdb.PrefixRange(s.keySpace.Pack(tuple.Tuple{key}))
	if err != nil {
		return err
	}

	var kvs []fdb.KeyValue
	var readVersion int64
	_, err = s.client.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		var e error
		readVersion, e = rtr.GetReadVersion().Get()
		if e != nil {
			return nil, e
		}
		kvs = []fdb.KeyValue{}
		slice, e := rtr.GetRange(keyRange, fdb.RangeOptions{}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		for _, v := range slice {
			kvs = append(kvs, v)
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	if len(kvs) > 0 {
		data, _, err := s.transformer.TransformFromStorage(kvs[0].Value, authenticatedDataString(key))
		if err != nil {
			return storage.NewInternalError(err.Error())
		}
		if err := appendListItem(v, data, uint64(readVersion), pred, s.codec, s.versioner, newItemFunc); err != nil {
			return err
		}
	}

	// update version with cluster level revision
	return s.versioner.UpdateList(listObj, uint64(readVersion), "", nil)

}

func (s Store) List(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}
	if err != nil {
		return err
	}
	keyRange, err := fdb.PrefixRange(s.keySpace.Pack(tuple.Tuple{key}))
	if err != nil {
		return err
	}

	var kvs []fdb.KeyValue
	var readVersion int64
	_, err = s.client.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		var e error
		readVersion, e = rtr.GetReadVersion().Get()
		if e != nil {
			return nil, e
		}
		kvs = []fdb.KeyValue{}
		slice, e := rtr.GetRange(keyRange, fdb.RangeOptions{}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		for _, v := range slice {
			kvs = append(kvs, v)
		}

		return nil, nil
	})
	if err != nil {
		return err
	}

	newItemFunc := getNewItemFunc(listObj, v)

	// take items from the response until the bucket is full, filtering as we go
	for _, kv := range kvs {

		data, _, err := s.transformer.TransformFromStorage(kv.Value, authenticatedDataString(kv.Key))
		if err != nil {
			return storage.NewInternalErrorf("unable to transform key %q: %v", kv.Key, err)
		}

		if err := appendListItem(v, data, uint64(readVersion), opts.Predicate, s.codec, s.versioner, newItemFunc); err != nil {
			return err
		}
	}

	return nil
}

func getNewItemFunc(listObj runtime.Object, v reflect.Value) func() runtime.Object {
	// For unstructured lists with a target group/version, preserve the group/version in the instantiated list items
	if unstructuredList, isUnstructured := listObj.(*unstructured.UnstructuredList); isUnstructured {
		if apiVersion := unstructuredList.GetAPIVersion(); len(apiVersion) > 0 {
			return func() runtime.Object {
				return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": apiVersion}}
			}
		}
	}

	// Otherwise just instantiate an empty item
	elem := v.Type().Elem()
	return func() runtime.Object {
		return reflect.New(elem).Interface().(runtime.Object)
	}
}

// appendListItem decodes and appends the object (if it passes filter) to v, which must be a slice.
func appendListItem(v reflect.Value, data []byte, rev uint64, pred storage.SelectionPredicate, codec runtime.Codec, versioner storage.Versioner, newItemFunc func() runtime.Object) error {
	obj, _, err := codec.Decode(data, nil, newItemFunc())
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	if err := versioner.UpdateObject(obj, rev); err != nil {
		klog.Errorf("failed to update object version: %v", err)
	}
	if matched, err := pred.Matches(obj); err == nil && matched {
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}
	return nil
}

func _unpack(t []byte) tuple.Tuple {
	i, e := tuple.Unpack(t)
	if e != nil {
		return nil
	}
	return i
}

func (s Store) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, precondtions *storage.Preconditions, tryUpdate storage.UpdateFunc, suggestion ...runtime.Object) error {
	panic("implement me")
}

func (s Store) Count(key string) (int64, error) {
	panic("implement me")
}


