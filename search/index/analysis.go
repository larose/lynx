package index

import (
	"unicode"
	"unicode/utf8"
)

type Token struct {
	Text []byte
}

type StandardTokenizer struct {
	input           []byte
	inputIndex      int
	token           *Token
	tokenBuffer     []rune
	tokenTextBuffer []byte
}

func NewStandardTokenizer() *StandardTokenizer {
	return &StandardTokenizer{
		token:           &Token{},
		tokenBuffer:     make([]rune, 0, 100),
		tokenTextBuffer: make([]byte, 100),
	}
}

func (t *StandardTokenizer) Reset(input []byte) {
	t.input = input
	t.inputIndex = 0
}

func runesToBytes(rs []rune, out []byte) ([]byte, []byte) {
	size := 0
	for _, r := range rs {
		size += utf8.RuneLen(r)
	}

	if cap(out) < size {
		out = make([]byte, size)
	}

	count := 0
	for _, r := range rs {
		count += utf8.EncodeRune(out[count:], r)
	}

	return out, out[:size]
}

// Token is valid until the next call to NextToken
func (t *StandardTokenizer) NextToken() (*Token, bool) {
	t.tokenBuffer = t.tokenBuffer[:0]

	for t.inputIndex < len(t.input) {
		r, size := utf8.DecodeRune(t.input[t.inputIndex:])

		// TODO: apply proper normalizatoin
		normalizedRune := unicode.ToLower(r)

		if unicode.IsSpace(normalizedRune) || unicode.IsPunct(normalizedRune) {
			if len(t.tokenBuffer) > 0 {
				t.tokenTextBuffer, t.token.Text = runesToBytes(t.tokenBuffer, t.tokenTextBuffer)
				return t.token, true
			}
		} else {
			t.tokenBuffer = append(t.tokenBuffer, normalizedRune)
		}

		t.inputIndex += size
	}

	if len(t.tokenBuffer) > 0 {
		t.tokenTextBuffer, t.token.Text = runesToBytes(t.tokenBuffer, t.tokenTextBuffer)
		return t.token, true
	}

	return nil, false
}
