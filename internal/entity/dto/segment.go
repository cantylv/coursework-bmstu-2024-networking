package dto

import (
	"time"

	"github.com/asaskevich/govalidator"
)

const (
	timestamptz = "2006-01-02 15:04:05+03:00"
) 

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
	Data []byte `json:"data" valid:"-"`
	SegmentId uint64 `json:"segment_id" valid:"is_non_negative, optional"`
	NumberOfSegments uint64 `json:"number_of_segments" valid:"is_non_negative"`
	DateSend string `json:"date_send" valid:"-"`
	Login string `json:"login" valid:"login_domain"`
	MessageId string `json:"message_id" valid:"uuidv4"`
	Error bool `json:"error" valid:"-"`
}

func (d *SegmentFromDatalinkLayer) Validate() (bool, error) {
	return govalidator.ValidateStruct(d)
}

func GetSegmentToDatalinkLayer(rawData []byte, numberOfSegments int, segmentId uint64, login string, messageId string) *SegmentToDatalinkLayer {
	return &SegmentToDatalinkLayer{
		Data: rawData,
		SegmentId: segmentId,
		NumberOfSegments: numberOfSegments,
		DateSend: time.Now().UTC().Format(timestamptz),
		Login: login,
		MessageId: messageId,
	}
}