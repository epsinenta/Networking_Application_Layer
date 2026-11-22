package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
)

func Decode(r *http.Request, v any) error {
	defer r.Body.Close()
	d := json.NewDecoder(io.LimitReader(r.Body, 256*1024))
	if err := d.Decode(v); err != nil {
		return err
	}
	return nil
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"code": "error", "message": msg})
}

func Only(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			Error(w, 405, "method not allowed")
			return
		}
		h(w, r)
	}
}
