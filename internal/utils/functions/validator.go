package functions

import (
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/asaskevich/govalidator"
)

const maxCountLetterInString = 2 << 14  // size of rune is 2 byte --> max length of msg == maxCountLetterInString * 2 (bytes)
var loginRegExpr = regexp.MustCompile(`^[a-zA-Zа-яА-ЯёЁ][a-zA-Zа-яА-ЯёЁ0-9]{1,20}$`)

func InitDTOValidator() {
	govalidator.SetFieldsRequiredByDefault(true)
	
	govalidator.TagMap["message_domain"] = func(message string) bool {
		countLetter := utf8.RuneCountInString(message)
		return countLetter > 0 && countLetter < maxCountLetterInString
	}

	govalidator.TagMap["login_domain"] = func(login string) bool {
		return loginRegExpr.MatchString(login)
	}

	govalidator.TagMap["segment_data_domain"] = func(data string) bool {
		return utf8.RuneCountInString(data) > 0
	}
	
	govalidator.TagMap["is_non_negative"] = func(numStr string) bool {
		num, err := strconv.Atoi(numStr)
		if err != nil {
			return false
		}
		return govalidator.IsNonNegative(float64(num))
	}
}