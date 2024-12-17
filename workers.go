package main

import (
	"GoLandFiles/utils"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Worker struct {
	WorkerID     int
	WorkerToDo   []int32
	WorkerRanges map[int][]int32
	Intermediate map[int32]int32
	mutex        sync.Mutex
	wait         sync.WaitGroup
}

func createKeyVal(data []int32) map[int32]int32 {
	result := make(map[int32]int32)
	for _, val := range data {
		result[val]++
	}
	fmt.Println("Il result è", result)
	return result
}

func (w *Worker) DistributedAndSortJob(arg *utils.ReduceArgs, reply *utils.ReduceReply) error {
	fmt.Printf("Worker %d processamento dati:\n", w.WorkerID)
	var wait sync.WaitGroup

	for diffID, diffRange := range w.WorkerRanges {
		if diffID == w.WorkerID {
			continue
		}

		wait.Add(1)
		go func(diffID int, diffRange []int32) {
			defer wait.Done()
			diffWorkerAdd := fmt.Sprintf("localhost:%d", 8000+diffID)
			client, err := rpc.Dial("tcp", diffWorkerAdd)
			if err != nil {
				log.Printf("Errore nella connessione con %d: %v", diffID, err)
				return
			}
			defer client.Close()

			temp := make(map[int32]int32)
			w.mutex.Lock()
			for key, val := range w.Intermediate {
				if isInRange(key, w.WorkerRanges[w.WorkerID]) && isInRange(key, diffRange) {
					temp[key] += val
					delete(w.Intermediate, key)
				}
			}
			w.mutex.Unlock()
			if len(temp) > 0 {
				sendArg := utils.WorkerArgs{
					Job:          temp,
					WorkerID:     w.WorkerID,
					WorkerRanges: w.WorkerRanges,
				}
				var reply utils.ReduceReply
				err = client.Call("Worker.ReceiveData", &sendArg, &reply)
				if err != nil {
					log.Printf("Errore chiamata rpc %d: %v", diffID, err)
					return
				}
			}
		}(diffID, diffRange)
	}
	wait.Wait()
	fmt.Printf("Worker %d processamento dati %v:\n", w.WorkerID, w.Intermediate)
	reply.Ack = "Riduzione completata"

	masterAdd := fmt.Sprintf("localhost:9999")
	client, err := rpc.Dial("tcp", masterAdd)
	if err != nil {
		log.Printf("Errore nella connessione con master %v", err)
		return err
	}
	defer client.Close()
	workers := utils.WorkerArgs{
		Job:      w.Intermediate,
		WorkerID: w.WorkerID,
	}
	var workerReply utils.WorkerReply
	err = client.Call("Master.ReceiveDataFromWorker", &workers, &workerReply)
	if err != nil {
		log.Printf("Errore nella connessione con master %v", err)
		return err
	}
	fmt.Printf("Dati inviati al master")
	return nil
}

func (w *Worker) ProcessJob(arg *utils.WorkerArgs, reply *utils.WorkerReply) error {
	w.WorkerID = arg.WorkerID
	w.WorkerRanges = arg.WorkerRanges
	w.WorkerToDo = arg.JobToDo
	fmt.Printf("Job da eseguire:", arg.JobToDo)
	w.Intermediate = createKeyVal(arg.JobToDo)
	workerArgs := utils.WorkerArgs{}
	workerArgs.Job = w.Intermediate

	reply.Ack = fmt.Sprintf("Job completato, %d", len(w.Intermediate))
	return nil
}

func (w *Worker) ReceiveData(arg *utils.WorkerArgs, reply *utils.WorkerReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for key, value := range arg.Job {
		if isInRange(key, w.WorkerRanges[w.WorkerID]) {
			w.Intermediate[key] += value
		}
	}
	reply.Ack = "Dati ricevuti"
	return nil
}

func isInRange(key int32, ranges []int32) bool {
	for _, val := range ranges {
		if val == key {
			return true
		}
	}
	return false
}

func main() {
	id := flag.Int("ID", 0, "ID worker")
	port := flag.Int("Port", 8000, "Porta base")
	flag.Parse()

	if *id <= 0 {
		fmt.Println("ID deve essere maggiore di 0")
		os.Exit(1)
	}
	address := fmt.Sprintf("localhost:%d", *port+*id)
	fmt.Printf("Avvio del worker %d sulla porta %s\n", *id, address)

	worker := new(Worker)
	server := rpc.NewServer()
	err := server.Register(worker)
	if err != nil {
		log.Fatal("Errore in ascolto %v", err)
	}
	listener, err := net.Listen("tcp", address)
	utils.CheckError(err)
	defer listener.Close()

	fmt.Printf("Worker %d su porta %s\n", *id, address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Errore durante l'accentazione della connessione %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
}
