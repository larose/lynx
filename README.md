# Lynx

A fast, low-memory, and embeddable search engine implementation in Go.

## Basic Usage

Here's a simple example of how to use Lynx:

```go
import (
    "github.com/larose/lynx/search"
    "github.com/larose/lynx/search/index"
    "github.com/larose/lynx/search/query"
)

// Initialize the index
indexWriter := index.NewIndexWriter("path/to/index/directory")

// Add documents
docs := []index.Document{
    {
        {Name: "id", FieldType: index.ByteFieldType, Value: []byte{0, 0, 0, 1}},
        {Name: "body", FieldType: index.TextFieldType, Value: []byte("This is a sample document")},
        {Name: "title", FieldType: index.TextFieldType, Value: []byte("Sample")},
    },
    // Add more documents...
}
indexWriter.AddDocuments(docs)

// Create a query
searchQuery := &query.BooleanNode{
    Clauses: []*query.BooleanClause{
        {
            Type: query.Should,
            Node: &query.TermNode{FieldName: "body", Term: []byte("sample")},
        },
    },
}

// Perform a search
indexReader, _ := index.NewIndexReader("path/to/index/directory")
collector := query.NewTopNCollector(10)
search.Search(searchQuery, indexReader, collector)

// Get results
results := collector.Get()
for _, result := range results {
    // Process each result...
}
```
