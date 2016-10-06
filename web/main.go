package web

import (
    "net/http"
    "log"
)

func Start() {
    log.Println("main")
    http.Handle("/css/", http.FileServer(http.Dir("template")))
    http.Handle("/js/", http.FileServer(http.Dir("template")))
    
    http.HandleFunc("/",indexHandler)
    http.ListenAndServe(":8888", nil)
}