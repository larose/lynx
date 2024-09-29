package search_test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/larose/lynx/search"
	"github.com/larose/lynx/search/index"
	"github.com/larose/lynx/search/query"
	"github.com/larose/lynx/search/utils"
	"github.com/stretchr/testify/assert"
)

type Item struct {
	id   uint32
	text string
}

type BatchIterator func() ([]Item, bool)

func NewBatchIterator(fileName string, batchSize int) (BatchIterator, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	scanner := bufio.NewScanner(file)
	batch := make([]Item, 0, batchSize)
	lineNumber := uint32(0)

	return func() ([]Item, bool) {
		for scanner.Scan() {
			line := scanner.Text()
			item := Item{id: lineNumber, text: line}
			batch = append(batch, item)
			lineNumber++

			if len(batch) == batchSize {
				output := batch
				batch = make([]Item, 0, batchSize) // Reset batch
				return output, true
			}
		}

		if len(batch) > 0 {
			output := batch
			batch = nil // No more data after this
			return output, true
		}

		file.Close()

		return nil, false
	}, nil
}

func convertItemToDocument(item Item) index.Document {
	idBuffer := make([]byte, 10)
	binary.BigEndian.PutUint32(idBuffer, item.id)

	idField := index.Field{
		FieldType: index.TextFieldType,
		Name:      "id",
		Value:     idBuffer,
	}
	bodyField := index.Field{
		FieldType: index.TextFieldType,
		Name:      "body",
		Value:     []byte(item.text),
	}

	return index.Document{idField, bodyField}
}

func initRandomParagraphsIndex() string {
	directory := filepath.Join("testdata", "directory")
	os.RemoveAll(directory)

	err := os.MkdirAll(directory, 0700)
	if err != nil {
		log.Fatal(err)
	}

	iterator, err := NewBatchIterator("paragraphs.txt", 2_000)
	if err != nil {
		log.Fatal(fmt.Errorf("Error creating batch iterator: %v\n", err))
	}

	indexWriter := index.NewIndexWriter(directory)

	for {
		batch, ok := iterator()
		if !ok {
			break
		}

		documents := make([]index.Document, len(batch))

		for i, item := range batch {
			document := convertItemToDocument(item)
			documents[i] = document
		}

		if err := indexWriter.AddDocuments(documents); err != nil {
			log.Fatal(err)
		}
	}

	return directory
}

func initSimpleIndex() string {
	directory := filepath.Join("testdata", "directory")
	os.RemoveAll(directory)

	err := os.MkdirAll(directory, 0700)
	if err != nil {
		log.Fatal(err)
	}

	indexWriter := index.NewIndexWriter(directory)

	{
		docs := []index.Document{
			[]index.Field{
				{Name: "id", FieldType: index.ByteFieldType, Value: utils.Uint64ToBytes(9)},
				{Name: "body", FieldType: index.TextFieldType, Value: []byte("This is a hello world. Business.")},
				{Name: "title", FieldType: index.TextFieldType, Value: []byte("Hello, world")},
			},
			[]index.Field{
				{Name: "id", FieldType: index.ByteFieldType, Value: utils.Uint64ToBytes(3)},
				{Name: "body", FieldType: index.TextFieldType, Value: []byte("After years of struggling to stay afloat, a beloved local business... business world")},
				{Name: "title", FieldType: index.TextFieldType, Value: []byte("Local Business Closes its Doors")},
			},
			[]index.Field{
				{Name: "id", FieldType: index.ByteFieldType, Value: utils.Uint64ToBytes(89)},
				{Name: "body", FieldType: index.TextFieldType, Value: []byte("This is an apple. This is an orange. This is a car.")},
				{Name: "title", FieldType: index.TextFieldType, Value: []byte("This is")},
			},
		}

		indexWriter.AddDocuments(docs)
	}

	{
		docs := []index.Document{
			[]index.Field{
				{Name: "id", FieldType: index.ByteFieldType, Value: utils.Uint64ToBytes(34)},
				{Name: "body", FieldType: index.TextFieldType, Value: []byte("Roger that")},
				{Name: "title", FieldType: index.TextFieldType, Value: []byte("Ok, this is ok")},
			},
		}

		indexWriter.AddDocuments(docs)
	}

	return directory
}

func TestSearchRootDisjunctionNode(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "body", Term: []byte("hello")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 1)

	value, err := indexReader.Value("id", results[0].DocId)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
}

func TestSearchRootTermNode(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.TermNode{FieldName: "body", Term: []byte("hello")}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 1)

	value, err := indexReader.Value("id", results[0].DocId)
	if err != nil {
		log.Fatal(err)
	}

	assert.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
}

func TestSearchBodyTwoDocuments(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "body", Term: []byte("business")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 2)

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(3), binary.BigEndian.Uint64(value))
	}

	{
		value, err := indexReader.Value("id", results[1].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
	}
}

func TestSearchTitleTwoDocuments(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "title", Term: []byte("is")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 2)

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(89), binary.BigEndian.Uint64(value))
	}

	{
		value, err := indexReader.Value("id", results[1].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(34), binary.BigEndian.Uint64(value))
	}
}

func TestSearchDeleteDocument(t *testing.T) {
	directory := initSimpleIndex()

	indexWriter := index.NewIndexWriter(directory)

	values := make([][]byte, 0, 1)
	values = append(values, utils.Uint64ToBytes(89))
	indexWriter.DeleteDocuments("id", values)

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "title", Term: []byte("is")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 1)

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(34), binary.BigEndian.Uint64(value))
	}
}

func TestSearchAcrossTwoFields(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "title", Term: []byte("is")},
			},
			{
				Type: query.Should,
				Node: &query.TermNode{FieldName: "body", Term: []byte("is")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 3)

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(89), binary.BigEndian.Uint64(value))
	}

	{
		value, err := indexReader.Value("id", results[1].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
	}

	{
		value, err := indexReader.Value("id", results[2].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(34), binary.BigEndian.Uint64(value))
	}
}

func TestSearchConjunctive(t *testing.T) {
	directory := initSimpleIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Must,
				Node: &query.TermNode{FieldName: "title", Term: []byte("is")},
			},
			{
				Type: query.Must,
				Node: &query.TermNode{FieldName: "body", Term: []byte("that")},
			},
		},
	}

	collector := query.NewTopNCollector(10)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 1)

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint64(34), binary.BigEndian.Uint64(value))
	}
}

func TestSearchManyDocuments(t *testing.T) {
	directory := initRandomParagraphsIndex()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	_query := &query.BooleanNode{
		Clauses: []*query.BooleanClause{
			{
				Type: query.Must,
				Node: &query.TermNode{FieldName: "body", Term: []byte("the")},
			},
		},
	}

	collector := query.NewTopNCollector(1_000_000)

	err = search.Search(_query, indexReader, collector)
	if err != nil {
		log.Fatal(err)
	}

	results := collector.Get()

	assert.Len(t, results, 27683)

	// Top 3

	{
		value, err := indexReader.Value("id", results[0].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint32(21222), binary.BigEndian.Uint32(value))
	}

	{
		value, err := indexReader.Value("id", results[1].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint32(20515), binary.BigEndian.Uint32(value))
	}

	{
		value, err := indexReader.Value("id", results[2].DocId)
		if err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, uint32(23163), binary.BigEndian.Uint32(value))
	}
}
