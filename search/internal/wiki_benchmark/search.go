package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/larose/lynx/search"
	"github.com/larose/lynx/search/index"
	"github.com/larose/lynx/search/query"
)

type Query struct {
	terms [][]byte
}

func _search() {

	queries := make([]*Query, 0, 10)

	queries = append(queries, &Query{terms: [][]byte{[]byte("the")}})
	queries = append(queries, &Query{terms: [][]byte{[]byte("griffith"), []byte("observatory")}})
	queries = append(queries, &Query{terms: [][]byte{[]byte("bowel"), []byte("obstruction")}})
	queries = append(queries, &Query{terms: [][]byte{[]byte("vicenza"), []byte("italy")}})

	stopProfiler := startCpuProfiler("search.cpu.pprof")
	defer stopProfiler()

	indexReader, err := index.NewIndexReader(directory)
	if err != nil {
		log.Fatal(err)
	}

	for f := 0; f < 1; f++ {
		for _, _query := range queries {
			var best time.Duration
			best = math.MaxInt64
			for i := query.MatchType(0); i <= query.Must; i++ {
				for j := 0; j < 10; j++ {
					clauses := make([]*query.BooleanClause, 0, len(_query.terms))

					for _, term := range _query.terms {
						termNode := &query.TermNode{FieldName: "body", Term: term}

						clause := &query.BooleanClause{
							Type: i,
							Node: termNode,
						}

						clauses = append(clauses, clause)
					}

					q := &query.BooleanNode{Clauses: clauses}

					start := time.Now()

					collector := query.NewTopNCollector(10)

					err := search.Search(q, indexReader, collector)
					if err != nil {
						log.Fatal(err)
					}

					_ = collector.Get()

					elapsed := time.Since(start)

					if elapsed < best {
						best = elapsed
					}
				}

				var matchTypeName string
				if i == query.Should {
					matchTypeName = "should"
				} else {
					matchTypeName = "must"
				}

				fmt.Printf("%s %s: %d us\n", _query.terms, matchTypeName, best.Microseconds())
			}
		}
	}
}
