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
	fmt.Printf("Worker %d: inizio riduzione dati.\n", w.WorkerID)

	// Mappa temporanea per accumulare dati da inviare ad altri workers
	dataToSend := make(map[int]map[int32]int32)

	w.mutex.Lock()
	for key, val := range w.Intermediate {
		for diffID, diffRange := range w.WorkerRanges {
			if diffID != w.WorkerID && isInRange(key, diffRange) {
				if _, exists := dataToSend[diffID]; !exists {
					dataToSend[diffID] = make(map[int32]int32)
				}
				dataToSend[diffID][key] = val
				delete(w.Intermediate, key)
				break
			}
		}
	}
	w.mutex.Unlock()

	// Invio dei dati accumulati ai workers destinatari
	var wg sync.WaitGroup
	for diffID, data := range dataToSend {
		wg.Add(1)
		go func(diffID int, data map[int32]int32) {
			defer wg.Done()
			diffWorkerAddr := fmt.Sprintf("localhost:%d", 8000+diffID)
			client, err := rpc.Dial("tcp", diffWorkerAddr)
			if err != nil {
				log.Printf("Errore connessione worker %d: %v", diffID, err)
				return
			}
			defer client.Close()

			args := utils.WorkerArgs{
				Job:      data,
				WorkerID: w.WorkerID,
			}
			var response utils.ReduceReply
			if err := client.Call("Worker.ReceiveData", &args, &response); err != nil {
				log.Printf("Errore chiamata RPC al worker %d: %v", diffID, err)
			}
		}(diffID, data)
	}
	wg.Wait()

	// Invio finale al master
	masterAddr := "localhost:9999"
	client, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		log.Printf("Errore connessione con il master: %v", err)
		return err
	}
	defer client.Close()

	args := utils.WorkerArgs{
		Job:      w.Intermediate,
		WorkerID: w.WorkerID,
	}
	if err := client.Call("Master.ReceiveDataFromWorker", &args, reply); err != nil {
		log.Printf("Errore chiamata RPC al master: %v", err)
		return err
	}
	reply.Ack = "Riduzione completata e dati inviati al master"
	return nil
}

func (w *Worker) Execute(arg *utils.WorkerArgs, reply *utils.WorkerReply) error {
	w.WorkerID = arg.WorkerID
	w.WorkerRanges = arg.WorkerRanges
	w.WorkerToDo = arg.ToDo
	fmt.Printf("Job da eseguire: ", arg.ToDo)
	w.Intermediate = createKeyVal(arg.ToDo)
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
	if len(ranges) == 0 {
		return false // non ci sono intervalli da controllare
	}
	return key >= ranges[0] && key <= ranges[len(ranges)-1] // ranges[0] = min val; ranges[len(ranges)-1] = max val
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
