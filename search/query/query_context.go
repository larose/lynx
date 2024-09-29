package query

import (
	"bytes"
)

type QueryField struct {
	name  string
	terms [][]byte
}

type QueryContext struct {
	Fields []*QueryField
}

func (c *QueryContext) RegisterTerm(fieldName string, term []byte) (int, int) {
	for i, field := range c.Fields {
		if field.name == fieldName {
			for j, _term := range field.terms {
				if bytes.Equal(_term, term) {
					return i, j
				}
			}

			j := len(field.terms)
			field.terms = append(field.terms, term)

			return i, j
		}
	}

	i := len(c.Fields)

	terms := make([][]byte, 1, 10)
	terms[0] = term

	c.Fields = append(c.Fields, &QueryField{name: fieldName, terms: terms})

	return i, 0
}
