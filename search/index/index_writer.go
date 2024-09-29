package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"golang.org/x/exp/rand"
)

type IndexWriter struct {
	directory string
	mutex     sync.RWMutex
	tokenizer *StandardTokenizer
}

type Commit struct {
	SegmentIds []uint32 `json:"segmentIds"`
	DeletedId  *uint32  `json:"deletedId,omitempty"`
}

func NewIndexWriter(directory string) *IndexWriter {
	return &IndexWriter{
		directory: directory,
		tokenizer: NewStandardTokenizer(),
	}
}

func (writer *IndexWriter) AddDocuments(docs []Document) error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	segmentComponentWriters := make([]SegmentComponentWriter, 0, 10)

	segmentComponentWriters = append(segmentComponentWriters, newInvertedIndexWriter(), newStoreWriter())

	for docId, doc := range docs {
		for _, segmentComponentWriter := range segmentComponentWriters {
			segmentComponentWriter.Doc(DocumentId(docId))
		}

		for _, field := range doc {
			for _, segmentComponentWriter := range segmentComponentWriters {
				segmentComponentWriter.Field(field.Name, field.Value)
			}

			switch field.FieldType {
			case TextFieldType:
				{
					writer.tokenizer.Reset(field.Value)
					for {
						token, ok := writer.tokenizer.NextToken()
						if !ok {
							break
						}

						for _, segmentComponentWriter := range segmentComponentWriters {
							segmentComponentWriter.Term(token.Text)
						}

					}

				}
			case ByteFieldType:
				{
					for _, segmentComponentWriter := range segmentComponentWriters {
						segmentComponentWriter.Term(field.Value)
					}
				}
			default:
				return fmt.Errorf("unknown field type %d", field.FieldType)
			}

			for _, segmentComponentWriter := range segmentComponentWriters {
				segmentComponentWriter.EndField()
			}
		}
	}

	newSegmentId := rand.Uint32()

	for _, segmentComponentWriter := range segmentComponentWriters {
		err := segmentComponentWriter.Write(writer.directory, strconv.FormatUint(uint64(newSegmentId), 10))
		if err != nil {
			return err
		}

	}

	commit, err := readCommit(writer.directory)
	if err != nil {
		return err
	}

	segmentIds := commit.SegmentIds

	segmentIds = append(segmentIds, newSegmentId)

	return writer.commit(segmentIds, commit.DeletedId)
}

func (writer *IndexWriter) commit(segmentIds []uint32, deletedId *uint32) error {
	tempFilePath := filepath.Join(writer.directory, ".commit")
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}

	defer tempFile.Close()

	commit := Commit{
		SegmentIds: segmentIds,
		DeletedId:  deletedId,
	}

	encoder := json.NewEncoder(tempFile)

	err = encoder.Encode(commit)
	if err != nil {
		return err
	}

	commitFilePath := filepath.Join(writer.directory, "commit")
	err = os.Rename(tempFilePath, commitFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (writer *IndexWriter) DeleteDocuments(fieldName string, values [][]byte) error {
	indexReader, err := NewIndexReader(writer.directory)
	if err != nil {
		return err
	}

	// TODO: This is a merge behavior here, we'll need to have the right structure for this and for other merges, definitevely not inline here

	docIdsToDelete, err := indexReader.SearchByExactValues(fieldName, values)
	if err != nil {
		return err
	}

	commit, err := readCommit(writer.directory)
	if err != nil {
		return err
	}

	var deletedReader DeletedReader
	var nextDeletedId uint32

	if commit.DeletedId == nil {
		deletedReader = newNullDeletedReader()
	} else {
		deletedReader, err = newFileDeletedReader(writer.directory, strconv.FormatUint(uint64(*commit.DeletedId), 10))
		if err != nil {
			return err
		}

		nextDeletedId = *commit.DeletedId + 1
	}

	deletedDocIdsBySegment := make(map[uint32]*roaring.Bitmap)

	for _, docId := range docIdsToDelete {
		segmentId := ToSegmentId(docId)

		deletedDocIdsForSegment, exists := deletedDocIdsBySegment[segmentId]
		if !exists {
			deletedDocIdsForSegment, err = deletedReader.GetDeletedDocIdsForSegment(segmentId)
			if err != nil {
				return err
			}
			if deletedDocIdsForSegment == nil {
				deletedDocIdsForSegment = roaring.NewBitmap()
			}

			deletedDocIdsBySegment[segmentId] = deletedDocIdsForSegment
		}

		deletedDocIdsForSegment.Add(uint32(toLocalDocId(docId)))
	}

	deletedWriter := newDeletedWriter()

	deletedWriter.DeletedDocs(deletedDocIdsBySegment)

	deletedWriter.Write(writer.directory, strconv.FormatUint(uint64(nextDeletedId), 10))

	writer.commit(commit.SegmentIds, &nextDeletedId)

	return nil
}
