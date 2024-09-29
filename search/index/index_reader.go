package index

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	"github.com/RoaringBitmap/roaring/v2"
)

func readCommit(directory string) (*Commit, error) {
	dirEntries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	for _, dirEntry := range dirEntries {
		if !dirEntry.Type().IsRegular() {
			continue
		}

		name := dirEntry.Name()

		if name != "commit" {
			continue
		}

		commitFile, err := os.Open(filepath.Join(directory, name))
		if err != nil {
			return nil, err
		}

		defer commitFile.Close()

		decoder := json.NewDecoder(commitFile)

		var commit Commit
		if err := decoder.Decode(&commit); err != nil {
			return nil, err
		}
		return &commit, nil
	}

	return &Commit{SegmentIds: make([]uint32, 0)}, nil
}

func ToGlobalDocId(segmentId, localDocId uint32) uint64 {
	globalDocId := uint64(segmentId)<<32 | uint64(localDocId)
	return globalDocId
}

func ToSegmentId(docId uint64) uint32 {
	return uint32(docId >> 32)
}

func toLocalDocId(docId uint64) DocumentId {
	return DocumentId(uint32(docId))
}

type IndexReader struct {
	SegmentReaders []*SegmentReader
}

func NewIndexReader(directory string) (*IndexReader, error) {
	commit, err := readCommit(directory)
	if err != nil {
		return nil, err
	}

	segmentReaders := make([]*SegmentReader, 0, len(commit.SegmentIds))

	var deletedReader DeletedReader

	if commit.DeletedId == nil {
		deletedReader = newNullDeletedReader()
	} else {
		deletedReader, err = newFileDeletedReader(directory, strconv.FormatUint(uint64(*commit.DeletedId), 10))
		if err != nil {
			return nil, err
		}
	}

	for _, segmentId := range commit.SegmentIds {
		deletedDocIdsForSegment, err := deletedReader.GetDeletedDocIdsForSegment(segmentId)
		if err != nil {
			return nil, err
		}

		if deletedDocIdsForSegment == nil {
			deletedDocIdsForSegment = roaring.NewBitmap()
		}

		segmentReaders = append(segmentReaders, newSegmentReader(directory, segmentId, deletedDocIdsForSegment))
	}

	return &IndexReader{
		SegmentReaders: segmentReaders,
	}, nil
}

func (reader *IndexReader) SearchByExactValues(fieldName string, values [][]byte) ([]uint64, error) {
	results := make([]uint64, 0, 100)

	for _, segmentReader := range reader.SegmentReaders {
		dictionaryReader, err := segmentReader.DictionaryReader(fieldName)
		if err != nil {
			return nil, err
		}

		fieldFreqsReader, err := segmentReader.FieldFreqsReader(fieldName)
		if err != nil {
			return nil, err
		}

		segmentDocIds := roaring.NewBitmap()

		for _, value := range values {
			termInfo := dictionaryReader.Get(value)
			if termInfo == nil {
				continue
			}

			docId := DocumentId(0)
			it := fieldFreqsReader.TermFreqsIterator(termInfo)
			if !it.NextShallow(docId) {
				continue
			}

			for it.Next(docId) {
				segmentDocIds.Add(uint32(it.DocId()))
				docId = it.DocId() + 1
			}
		}

		for _, docId := range segmentDocIds.ToArray() {
			globalDocId := ToGlobalDocId(segmentReader.Id, uint32(docId))

			results = append(results, globalDocId)
		}
	}

	return results, nil

}

func (reader *IndexReader) Value(fieldName string, docId uint64) ([]byte, error) {
	segmentId := ToSegmentId(docId)
	localDocId := toLocalDocId(docId)

	for _, segmentReader := range reader.SegmentReaders {
		if segmentReader.Id == segmentId {
			fieldStoreReader, err := segmentReader.storeReader.GetFieldStoreReader(fieldName)
			if err != nil {
				return nil, err
			}

			return fieldStoreReader.Value(localDocId), nil
		}
	}

	return nil, nil
}
