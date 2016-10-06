package main

import (
	"runtime"
	"os"
	"os/signal"
	"syscall"
	"runtime/debug"
	"fmt"
	"time"
	"github.com/lycying/pitydb/web"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	web.Start()

	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL, syscall.SIGUSR1)

	for {
		cmd := <-s
		if cmd == syscall.SIGKILL {
			break
		} else if cmd == syscall.SIGUSR1 {
			unixtime := time.Now().Unix()
			path := fmt.Sprintf("./heapdump-luoli-%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}
}

