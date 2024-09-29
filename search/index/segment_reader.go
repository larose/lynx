package index

import (
	"strconv"

	"github.com/RoaringBitmap/roaring/v2"
)

type SegmentReader struct {
	DeletedDocIds     *roaring.Bitmap
	dictionaryReaders map[string]*DictionaryReader
	DocLengthReader   *DocFieldLengthReader
	directory         string
	Id                uint32
	IdString          string
	fieldFreqsReaders map[string]*FieldFreqsReader
	storeReader       *StoreReader
}

func newSegmentReader(directory string, segmentId uint32, deletedDocIds *roaring.Bitmap) *SegmentReader {
	segment := strconv.FormatUint(uint64(segmentId), 10)
	return &SegmentReader{
		DeletedDocIds:     deletedDocIds,
		dictionaryReaders: make(map[string]*DictionaryReader),
		directory:         directory,
		DocLengthReader:   newDocFieldLengthReader(directory, segment),
		Id:                segmentId,
		IdString:          segment,
		fieldFreqsReaders: make(map[string]*FieldFreqsReader),
		storeReader:       newStoreReader(directory, segment),
	}
}

func (reader *SegmentReader) DictionaryReader(fieldName string) (*DictionaryReader, error) {
	dictionaryReader, exists := reader.dictionaryReaders[fieldName]
	if !exists {
		var err error
		dictionaryReader, err = newDictionaryReader(reader.directory, reader.IdString, fieldName)
		if err != nil {
			return nil, err
		}

		reader.dictionaryReaders[fieldName] = dictionaryReader
	}

	return dictionaryReader, nil
}

func (reader *SegmentReader) DocCountAndSumTermFreqForField(fieldName string) (uint32, uint64, error) {
	fieldStatsReader, err := newFieldStatsReader(reader.directory, reader.IdString, fieldName)
	if err != nil {
		return 0, 0, err
	}

	docCount, sumTermFreq, err := fieldStatsReader.Read()
	if err != nil {
		return 0, 0, err
	}

	return docCount, sumTermFreq, nil
}

func (reader *SegmentReader) FieldFreqsReader(fieldName string) (*FieldFreqsReader, error) {
	fieldFreqsReader, exists := reader.fieldFreqsReaders[fieldName]
	if !exists {
		var err error
		fieldFreqsReader, err = newFieldFreqsReader(reader.directory, reader.IdString, fieldName)
		if err != nil {
			return nil, err
		}

		reader.fieldFreqsReaders[fieldName] = fieldFreqsReader
	}

	return fieldFreqsReader, nil
}
