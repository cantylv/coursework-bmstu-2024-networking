package dto

import "github.com/asaskevich/govalidator"

// DTOs related within Application Layer
type MessageFromAppLayer struct {
	Message string `json:"message" valid:"message_domain"`
	Login string `json:"login" valid:"login_domain"`
}

type MessageToAppLayer struct {
	Message string `json:"message" valid:"-"`
	Login string `json:"login" valid:"-"`
	Date string `json:"date" valid:"-"`
	Error bool `json:"error" valid:"-"`
}

func (d *MessageFromAppLayer) Validate() (bool, error) {
	return govalidator.ValidateStruct(d)
}