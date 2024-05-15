package functions

import (
	"net/http"
	"encoding/json"
)

func JsonResponse(w http.ResponseWriter, data interface{}) http.ResponseWriter {
	w.Header().Set("Content-Type", "application/json")
	body, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return w
	}

	_, err = w.Write(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return w
	}
	return w
}


func ErrorResponse(w http.ResponseWriter, responseError error, codeStatus int) http.ResponseWriter {
	w.Header().Set("Content-Type", "application/json")
	errObject := map[string]string{"detail": responseError.Error()}
	body, err := json.Marshal(errObject)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return w
	}

	w.WriteHeader(codeStatus)
	_, err = w.Write(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	return w
}