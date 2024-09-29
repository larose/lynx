package index

// import (
// 	"log"
// 	"os"
// 	"testing"

// 	"github.com/RoaringBitmap/roaring/v2"
// )

// func TestSegment(t *testing.T) {
// 	filename := "segment"
// 	err := os.Remove(filename)
// 	if err != nil {
// 		if !os.IsNotExist(err) {
// 			log.Fatal(err)
// 		}
// 	}

// 	fileWriter, err := newFileWriter(filename)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	kvStoreWriter, err := newKVStoreWriter(fileWriter)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	docsToTermWriter, err := newDocsToTermsWriter(kvStoreWriter)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	{
// 		docIds := roaring.NewBitmap()
// 		docIds.Add(5)
// 		docIds.Add(2)
// 		docIds.Add(8)

// 		docsToTermWriter.Add([]byte("animal"), docIds)
// 	}

// 	{
// 		docIds := roaring.NewBitmap()
// 		docIds.Add(9)
// 		docIds.Add(30)
// 		docIds.Add(2)

// 		docsToTermWriter.Add([]byte("magic"), docIds)
// 	}

// 	{
// 		docIds := roaring.NewBitmap()
// 		docIds.Add(39)
// 		docIds.Add(0)
// 		docIds.Add(5)

// 		docsToTermWriter.Add([]byte("version"), docIds)
// 	}
// }
