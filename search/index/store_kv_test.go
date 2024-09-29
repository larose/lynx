package index

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Item struct {
	key, value []byte
}

func TestKVStore(t *testing.T) {
	os.MkdirAll("testdata", 0700)
	basename := filepath.Join("testdata", "test_kvstore")
	os.Remove(basename + ".data")
	os.Remove(basename + ".index")

	writer, err := newKVStoreWriter(basename)
	if err != nil {
		t.Fatalf("failed to create KVStoreWriter: %v", err)
	}
	defer writer.Close()

	testData := []Item{
		{key: []byte("apple"), value: []byte("fruit")},
		{key: []byte("carrot"), value: []byte("vegetable")},
		{key: []byte("dog"), value: []byte("animal")},
		{key: []byte("foo"), value: []byte("bar")},
		{key: []byte("hello"), value: []byte("world")},
	}

	for _, item := range testData {
		if err := writer.Append(item.key, item.value); err != nil {
			t.Fatalf("failed to append key-value pair (%s, %s): %v", item.key, item.value, err)
		}
	}

	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	reader, err := newKVStoreReader(basename)
	if err != nil {
		t.Fatalf("failed to create KVStoreReader: %v", err)
	}
	defer reader.Close()

	for _, item := range testData {
		// if !bytes.Equal(item.key, []byte("foo")) {
		// 	continue
		// }

		value := reader.Get(item.key)
		assert.Equal(t, item.value, value)
	}

	// Test non-existing key
	value := reader.Get([]byte("9661c61e"))
	assert.Nil(t, value)
}
