package main

import (
	"GoLandFiles/utils"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Definiamo la struttura per le richieste dei chunk da parte dei mapper
type DatasetRequest struct {
	Dataset []string
}

// definiamo una struttura per i messaggi di ricezione da parte dei mapper
type Response struct {
	Message string
}

// definiamo la struttura in cui definiamo gli addresses dei mapper
type RegisterMapperRequest struct {
	Address string
}

type Master struct {
	mappers []string
	mutex   sync.Mutex
}

// funzione principale del master, divide in chunks e invia ai mapper in modo sincrono
func (m *Master) Master(request utils.DatasetInput, reply *utils.DatasetOutput) error {
	log.Printf("Ricevuto dataset: %v", request.Data)

	// dividiamo il dataset in chunks
	//chunkSize := len(request.Data) / len(m.mappers)
	//log.Printf("1) chunkSize: %v", chunkSize)
	//if len(request.Data)%len(m.mappers) != 0 {
	//	chunkSize++
	//}
	//log.Printf("2) chunkSize: %v", chunkSize)
	chunkSize := 5
	chunks := divideIntoChunks(request.Data, chunkSize)
	log.Printf("Dataset diviso in chunks: %v", chunks)
	//messaggio di ricezione per il client
	reply.Message = "Dataset diviso in chunks"

	// invio dei chunks ai mappers in modo sincrono
	var wait sync.WaitGroup
	for i, mapperAddress := range m.mappers {
		if i >= len(chunks) {
			break
		}
		wait.Add(1)
		go func(address string, chunk []string) {
			defer wait.Done()
			client, err := rpc.Dial("tcp", address)
			utils.CheckError(err)
			defer client.Close()

			chunkRequest := DatasetRequest{Dataset: chunk}
			var mapperResponse Response
			err = client.Call("Mapper.RecieveChunk", chunkRequest, &mapperResponse)
			if err != nil {
				log.Fatal("Errore inviando il chunk al mapper %s %v", address, err)
			}
			log.Printf("Mapper %s ha confermato la ricezione: %v\n", address, mapperResponse.Message)
		}(mapperAddress, chunks[i])
	}
	wait.Wait()
	log.Printf("Tutti i mapper hanno ricevuto i chunks")
	reply.Message = "Dataset diviso e chunk inviati"
	return nil
}

func (m *Master) RegisterMapper(req RegisterMapperRequest, res *Response) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mappers = append(m.mappers, req.Address)
	res.Message = "Mapper con indirizzo registrato"
	log.Printf("Mapper registro: %v", req.Address)
	return nil
}

func divideIntoChunks(input []string, size int) [][]string {
	var chunks [][]string
	for size < len(input) {
		input, chunks = input[size:], append(chunks, input[0:size])
	}
	chunks = append(chunks, input)
	return chunks
}

func main() {
	master := &Master{mappers: []string{}}
	err := rpc.Register(master)
	utils.CheckError(err)

	listener, err := net.Listen("tcp", "localhost:9999")
	utils.CheckError(err)
	defer listener.Close()

	log.Printf("Master in ascolto sulla porta 9999")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Errore nella connessione %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
