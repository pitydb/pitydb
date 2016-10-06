package web

import (
	"net/http"
	"log"
	"html/template"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("template/html/index.html")
	if (err != nil) {
		log.Println(err)
	}
	t.Execute(w, nil)

}
