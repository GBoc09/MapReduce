package main

import (
	"GoLandFiles/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Master struct {
	CollectedData []utils.WorkerData
	mutex         sync.Mutex
	results       map[int32]int32
}

func (m *Master) ReceiveDataFromWorker(arg *utils.WorkerArgs, reply *utils.WorkerReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	workerData := utils.WorkerData{
		WorkerID: arg.WorkerID,
		Data:     arg.Job,
	}
	m.CollectedData = append(m.CollectedData, workerData)
	reply.Ack = "Dati ricevuti dai workers"
	fmt.Println("I dati sono:", m.CollectedData)
	return nil

}

func translateDataToArray(data []utils.WorkerData) []int32 {
	var result []int32
	for _, worker := range data {
		for key, value := range worker.Data {
			for i := 0; i < int(value); i++ {
				result = append(result, key)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

func maxAndRanges(dataset []int32, workersNum int) map[int][]int32 {
	max := dataset[0]
	for _, value := range dataset {
		if value > max {
			max = value
		}
	}
	fmt.Println("Max value in the dataset: %d", max)

	ranges := make(map[int][]int32)
	item := int(max)
	size := item / workersNum
	start := 1
	for i := 1; i <= workersNum; i++ {
		end := start + size - 1
		if i == workersNum {
			end = item
		}
		list := make([]int32, end-start+1)
		for j := start; j <= end; j++ {
			list = append(list, int32(j))
		}
		ranges[i] = list
		start = end + 1
	}
	return ranges
}
func divideIntoChunks(input []int32, size int) [][]int32 {
	var chunks [][]int32
	for size < len(input) {
		input, chunks = input[size:], append(chunks, input[0:size])
	}
	chunks = append(chunks, input)
	return chunks
}

// funzione principale del master, divide in chunks e invia ai mapper in modo sincrono
func (m *Master) MasterReceiveData(request utils.DatasetInput, reply *utils.DatasetOutput) error {
	log.Printf("Ricevuto dataset: %v", request.Data)

	numWorkers := 5

	ranges := maxAndRanges(request.Data, numWorkers)
	fmt.Println("Range dei workers: %v", ranges)

	fmt.Println("I range dei worker sono:", ranges)

	var workerData = make(map[int][]int32)

	chunks := divideIntoChunks(request.Data, numWorkers)
	for i, value := range chunks {
		workerID := (i % numWorkers) + 1
		workerData[workerID] = append(workerData[workerID], value...)
	}

	var wg sync.WaitGroup
	var mutex sync.Mutex

	for workerID, data := range workerData {
		wg.Add(1)
		go func(workerID int, data []int32) {
			defer wg.Done()

			workerAddr := fmt.Sprintf("localhost:%d", 8000+workerID)
			workerConn, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al worker %d: %v", workerID, err)
				return
			}
			defer workerConn.Close()

			workerArgs := utils.WorkerArgs{
				ToDo:         data,
				WorkerID:     workerID,
				WorkerRanges: ranges,
			}
			var workerReply utils.WorkerReply
			err = workerConn.Call("Worker.Execute", &workerArgs, &workerReply)
			if err != nil {
				log.Printf("Errore nella connessione al worker %d: %v", workerID, err)
				return
			}
			mutex.Lock()
			m.CollectedData = append(m.CollectedData, utils.WorkerData{
				WorkerID: workerID,
				Data:     workerReply.Data,
			})
			mutex.Unlock()

			fmt.Printf("Ricevuto dataset: %v workerID: %v", workerID, workerReply.Ack)
		}(workerID, data)
	}

	wg.Wait()

	reducerWorkers(ranges)

	finalArray := translateDataToArray(m.CollectedData)
	fmt.Printf("Risultato finale inviato al client %v", finalArray)

	reply.FinalData = finalArray
	reply.Ack = "Dati elaborati con successo"
	if err := writesToFile(finalArray); err != nil {
		return err
	}
	return nil
}

func writesToFile(finalArray []int32) error {
	file, err := os.Create("result.txt")
	if err != nil {
		return fmt.Errorf("Errore nella creazione del file: %v", err)
	}
	defer file.Close()
	var builder strings.Builder
	for _, num := range finalArray {
		builder.WriteString(fmt.Sprintf("%d ", num))
	}
	_, err = file.WriteString(strings.TrimSpace(builder.String()))
	return err
}

func reducerWorkers(workerRanges map[int][]int32) {
	var wait sync.WaitGroup
	for wokerID := range workerRanges {
		wait.Add(1)
		go func(workerID int) {
			defer wait.Done()
			workerAddr := fmt.Sprintf("localhost:%d", 8000+workerID)
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al worker %d: %v", workerID, err)
				return
			}
			defer client.Close()

			reduceArgs := utils.WorkerArgs{}
			reduceReply := utils.WorkerReply{}
			err = client.Call("Worker.DistributedAndSortJob", &reduceArgs, &reduceReply)
			if err != nil {
				log.Printf("Errore nella chiamata RPC %d: %v", workerID, err)
			}
			fmt.Printf("Worker  %d ha completato la riduzione %v\n", wokerID, reduceReply.Ack)
		}(wokerID)
	}
	wait.Wait()
}

func waitForCompletion(master *Master) {
	for {
		master.mutex.Lock()
		if len(master.CollectedData) > 0 {
			master.mutex.Unlock()
			break
		}
		master.mutex.Unlock()
		time.Sleep(1 * time.Second)
	}
	log.Println("Tutti i job sono stati completati")
}

func main() {
	master := new(Master)
	server := rpc.NewServer()
	err := server.Register(master)
	utils.CheckError(err)

	stopChan := make(chan struct{})

	add := "localhost:9999"
	listener, err := net.Listen("tcp", add)
	utils.CheckError(err)
	defer listener.Close()

	go func() {

		log.Printf("MasterReceiveData in ascolto sulla porta 9999")
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-stopChan:
					return
				default:
					log.Printf("Errore nella connessione %v", err)
					continue
				}

			}
			go server.ServeConn(conn)
		}
	}()
	waitForCompletion(master)
	close(stopChan)
	log.Printf("Server chiuso\n")
}
