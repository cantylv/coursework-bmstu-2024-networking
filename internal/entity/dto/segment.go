package dto

import "github.com/asaskevich/govalidator"

// DTOs related within DataLink Layer
type SegmentToDatalinkLayer struct {
	Data []byte `json:"data" valid:"-"`
	SegmentId uint64 `json:"segment_id" valid:"-"`
	NumberOfSegments int `json:"number_of_segments" valid:"-"`
	DateSend string `json:"date_send" valid:"-"`
	Login string `json:"login" valid:"-"`
	MessageId string `json:"message_id" valid:"-"`
}

type SegmentFromDatalinkLayer struct {
	Data []byte `json:"data" valid:"segment_data_domain"`
	SegmentId uint64 `json:"segment_id" valid:"is_non_negative"`
	NumberOfSegments uint64 `json:"number_of_segments" valid:"is_non_negative"`
	DateSend string `json:"date_send" valid:"date_domain"`
	Login string `json:"login" valid:"login"`
	MessageId string `json:"message_id" valid:"uuidv4"`
	Error bool `json:"error" valid:"-"`
}

func (d *SegmentFromDatalinkLayer) Validate() (bool, error) {
	return govalidator.ValidateStruct(d)
}