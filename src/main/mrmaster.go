package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"os"
	"time"

	"../mr"
	uti "../utilities"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})
	if len(os.Args) < 2 {
		log.Fatal("No inputfiles...")
		os.Exit(1)
	}
	m := mr.MakeMaster(os.Args[1:], 10, uti.GetPath())
	for m.Done() == false {
		m.WorkerMutex.Lock()
		for _, w := range m.Workers {
			if w.State != mr.Failed {
				if !m.HeartBeat(w) {
					log.Info("Cant reach a worker with id = ", w.Id, " and port = ", w.Port, ". Marking as failed.")
					m.Workers[w.Id].State = mr.Failed
					// delete(m.Workers, w.Id)
					ind := -1
					for i, v := range m.WorkerQueue {
						if v == w.Id {
							ind = i
							break
						}
					}
					if ind != -1 {
						m.WorkerQueue[ind] = m.WorkerQueue[len(m.WorkerQueue)-1]
						m.WorkerQueue[len(m.WorkerQueue)-1] = ""
						m.WorkerQueue = m.WorkerQueue[:len(m.WorkerQueue)-1]
					}
				}
			}
		}

		m.DispatchTask()
		m.WorkerMutex.Unlock()
	}

	time.Sleep(time.Second)
}
