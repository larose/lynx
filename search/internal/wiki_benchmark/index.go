package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/larose/lynx/search/index"
)

const (
	batchSize        = 100_000
	numberOfArticles = 1_000_000
)

type Article struct {
	URL   string `json:"url"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

type ArticleIterator struct {
	file   *os.File
	reader *bufio.Reader
}

func newArticleIterator(filePath string) (*ArticleIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	return &ArticleIterator{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (it *ArticleIterator) NextBatch(maxItems int) ([]Article, error) {
	var batch []Article

	eof := false
	for {
		lineBytes, err := it.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				eof = true
			} else {
				return nil, err
			}
		}

		if len(lineBytes) > 0 {
			var article Article
			err = json.Unmarshal(lineBytes, &article)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				continue
			}

			batch = append(batch, article)
		}

		if eof || len(batch) == maxItems {
			break
		}
	}

	return batch, nil
}

func (it *ArticleIterator) Close() error {
	return it.file.Close()
}

func convertArticleToDocument(article Article) index.Document {
	return index.Document{
		index.Field{
			FieldType: index.ByteFieldType,
			Name:      "url",
			Value:     []byte(article.URL),
		},
		index.Field{
			FieldType: index.TextFieldType,
			Name:      "title",
			Value:     []byte(article.Title),
		},
		index.Field{
			FieldType: index.TextFieldType,
			Name:      "body",
			Value:     []byte(article.Body),
		},
	}
}

func _index() {
	stopProfiler := startCpuProfiler("index.cpu.pprof")
	defer stopProfiler()

	os.RemoveAll(directory)

	err := os.MkdirAll(directory, 0700)
	if err != nil {
		log.Fatal(err)
	}

	indexWriter := index.NewIndexWriter(directory)

	iterator, err := newArticleIterator("wiki-articles.jsonl")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer iterator.Close()

	totalProcessed := 0

	docs := make([]index.Document, 0, batchSize)

	for {
		log.Printf("totalProcessed = %d\n", totalProcessed)

		remaining := numberOfArticles - totalProcessed
		if remaining <= 0 {
			break
		}

		articles, err := iterator.NextBatch(min(batchSize, remaining))
		if err != nil {
			log.Fatal(err)
			break
		}

		if len(articles) == 0 {
			break
		}

		for _, article := range articles {
			doc := convertArticleToDocument(article)
			docs = append(docs, doc)
		}

		totalProcessed += len(articles)

		indexWriter.AddDocuments(docs)
		docs = docs[:0]
	}

	if totalProcessed != numberOfArticles {
		log.Fatalf("expected %d articles, but processed %d", numberOfArticles, totalProcessed)
	}
}
