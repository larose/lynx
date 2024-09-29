package index

import (
	"path/filepath"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/larose/lynx/search/utils"
)

type DeletedWriter struct {
	deletedDocIdsBySegment map[uint32]*roaring.Bitmap
}

func newDeletedWriter() *DeletedWriter {
	return &DeletedWriter{}
}

// TODO: Need to revisit, weird API, since we do most of the work outside of DeletedWriter
func (writer *DeletedWriter) DeletedDocs(deletedDocIdsBySegment map[uint32]*roaring.Bitmap) {
	writer.deletedDocIdsBySegment = deletedDocIdsBySegment
}

func (writer *DeletedWriter) Write(directory string, deletedId string) error {
	kvStoreWriter, err := newKVStoreWriter(filepath.Join(directory, "deleted."+deletedId))
	if err != nil {
		return err
	}

	sortedSegmentIds := make([]uint32, 0, len(writer.deletedDocIdsBySegment))
	for segmentId := range writer.deletedDocIdsBySegment {
		sortedSegmentIds = append(sortedSegmentIds, segmentId)
	}

	slices.Sort(sortedSegmentIds)

	for _, segmentId := range sortedSegmentIds {
		deletedDocsForSegment := writer.deletedDocIdsBySegment[segmentId]

		buffer, err := deletedDocsForSegment.ToBytes()
		if err != nil {
			return err
		}

		kvStoreWriter.Append(utils.Uint32ToBytes(segmentId), buffer)
	}

	return kvStoreWriter.Close()
}

type DeletedReader interface {
	GetDeletedDocIdsForSegment(segmentId uint32) (*roaring.Bitmap, error)
}

type NullDeletedReader struct {
}

func newNullDeletedReader() *NullDeletedReader {
	return &NullDeletedReader{}
}

func (reader *NullDeletedReader) GetDeletedDocIdsForSegment(segmentId uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

type FileDeletedReader struct {
	kvStoreReader *KVStoreReader
}

func newFileDeletedReader(directory, deletedId string) (*FileDeletedReader, error) {
	kvStoreReader, err := newKVStoreReader(filepath.Join(directory, "deleted."+deletedId))
	if err != nil {
		return nil, err
	}

	return &FileDeletedReader{kvStoreReader: kvStoreReader}, nil
}

func (reader *FileDeletedReader) GetDeletedDocIdsForSegment(segmentId uint32) (*roaring.Bitmap, error) {
	value := reader.kvStoreReader.Get(utils.Uint32ToBytes(segmentId))
	if value == nil {
		return nil, nil
	}

	deletedDocs := roaring.NewBitmap()
	err := deletedDocs.UnmarshalBinary(value)
	if err != nil {
		return nil, err
	}

	return deletedDocs, nil
}
