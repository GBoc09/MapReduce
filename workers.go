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

const masterPort = 9999 // porta del master

type Worker struct {
	WorkerID int
	ToDo     []int32
	Ranges   map[int][]int32
	MidSol   map[int32]int32
	mutex    sync.Mutex
}

func createKeyVal(data []int32) map[int32]int32 {
	result := make(map[int32]int32)
	for _, val := range data {
		result[val]++
	}
	fmt.Println("Il result è", result)
	return result
}
func isInRange(key int32, ranges []int32) bool {
	if len(ranges) == 0 {
		return false // non ci sono intervalli da controllare
	}
	return key >= ranges[0] && key <= ranges[len(ranges)-1] // ranges[0] = min val; ranges[len(ranges)-1] = max val
}

func (w *Worker) DistributedAndSortJob(arg *utils.ReducerArgs, reply *utils.Reply) error {
	fmt.Printf("Worker %d: Inizio riduzione dati.\n", w.WorkerID)

	var wg sync.WaitGroup

	// Distribuzione dei dati ad altri worker
	for diffID, diffRange := range w.Ranges {
		if diffID == w.WorkerID {
			continue
		}

		wg.Add(1)
		go func(diffID int, diffRange []int32) {
			defer wg.Done()
			if err := w.sendDataToWorker(diffID, diffRange); err != nil {
				log.Printf("Errore durante l'invio dei dati al Worker %d: %v", diffID, err)
			}
		}(diffID, diffRange)
	}

	// Attesa che tutti i goroutine completino
	wg.Wait()

	// Invio dei dati finali al master
	if err := w.sendDataToMaster(reply); err != nil {
		return err
	}

	reply.Ack = "Riduzione completata e dati inviati al master"
	return nil
}

// Invio dati ad altri worker
func (w *Worker) sendDataToWorker(diffID int, diffRange []int32) error {
	diffAddress := fmt.Sprintf("localhost:%d", 8000+diffID)
	client, err := rpc.Dial("tcp", diffAddress)
	if err != nil {
		return fmt.Errorf("connessione fallita a %s: %v", diffAddress, err)
	}
	defer client.Close()

	// Raccolta dati da inviare
	dataToSend := w.collectDataForRange(diffRange)

	if len(dataToSend) > 0 {
		sendArgs := utils.WorkerValues{
			Value:    dataToSend,
			WorkerID: w.WorkerID,
			Ranges:   w.Ranges,
		}
		var sendReply utils.WorkerReply
		if err := client.Call("Worker.ReceiveData", &sendArgs, &sendReply); err != nil {
			return fmt.Errorf("chiamata RPC fallita: %v", err)
		}
	}
	return nil
}

// Raccolta dati da inviare per un intervallo specifico
func (w *Worker) collectDataForRange(targetRange []int32) map[int32]int32 {
	dataToSend := make(map[int32]int32)
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for key, val := range w.MidSol {
		if !isInRange(key, w.Ranges[w.WorkerID]) && isInRange(key, targetRange) {
			dataToSend[key] = val
			delete(w.MidSol, key)
		}
	}
	return dataToSend
}

// Funzione per inviare i dati finali al master
func (w *Worker) sendDataToMaster(reply *utils.Reply) error {
	masterAddr := fmt.Sprintf("localhost:%d", masterPort)
	client, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		log.Printf("Errore connessione con il master: %v", err)
		return err
	}
	defer client.Close()

	args := utils.WorkerValues{
		Value:    w.MidSol,
		WorkerID: w.WorkerID,
	}

	if err := client.Call("Master.ReceiveDataFromWorker", &args, reply); err != nil {
		log.Printf("Errore chiamata RPC al master: %v", err)
		return err
	}

	return nil
}

func (w *Worker) Execute(arg *utils.WorkerValues, reply *utils.WorkerReply) error {
	w.WorkerID = arg.WorkerID
	w.Ranges = arg.Ranges
	w.ToDo = arg.ToDo
	fmt.Printf("Value da eseguire: ", arg.ToDo)
	w.MidSol = createKeyVal(arg.ToDo)
	workerArgs := utils.WorkerValues{}
	workerArgs.Value = w.MidSol

	reply.Ack = fmt.Sprintf("Value completato, %d", len(w.MidSol))
	return nil
}

func (w *Worker) ReceiveData(arg *utils.WorkerValues, reply *utils.WorkerReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for key, value := range arg.Value {
		if isInRange(key, w.Ranges[w.WorkerID]) {
			w.MidSol[key] += value
		}
	}
	reply.Ack = "Dati ricevuti"
	return nil
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
