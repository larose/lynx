package index

// Caller calls in order:
// - Doc()
// - Field()
// - Term()
// - Term()
// - ...
// - Field()
// - Term()
// - Term()
// - ...
// - Doc()
// - ...
// - Write()
type SegmentComponentWriter interface {
	Doc(docId DocumentId)
	Field(fieldName string, value []byte)
	EndField()
	Term(term []byte)
	Write(directory, segmentId string) error
}
