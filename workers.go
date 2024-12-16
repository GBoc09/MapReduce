package main

import (
	"GoLandFiles/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
)

// struttura per la ricezione dei chunk
type DatasetMapperRequest struct {
	Dataset []string
}
type DatasetResponse struct {
	Message string
}
type Mapper struct {
	Chunk  []string
	MinVal int
	MaxVal int
}
type Result struct {
	Min int
	Max int
}

type Reducer struct {
	ID          int
	Address     string
	Window      AcceptanceWindow
	AcceptedVal []int
	Mutex       sync.Mutex
}
type AcceptanceWindow struct {
	Low  int
	High int
}
type MapperToReducer struct {
	Values []int
}
type ReducerResponse struct {
	Message string
}

func (r *Reducer) AcceptValues(req MapperToReducer, res *ReducerResponse) error {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	for _, v := range req.Values {
		if v >= r.Window.Low && v <= r.Window.High {
			r.AcceptedVal = append(r.AcceptedVal, v)
		}
	}
	log.Printf("Reducer %s ha ricevuto i valori %v", r.Address, req.Values)
	res.Message = "Valori accettati"
	return nil
}

// funzione per la ricezione dei chunks
func (m *Mapper) ReceiveChunk(req DatasetMapperRequest, res *Response) error {
	log.Printf("Mapper avviato. In attesa di ricevere dati...")
	m.Chunk = req.Dataset
	log.Printf("Received chunk %v\n", m.Chunk)

	min, max, err := findMinAndMax(m.Chunk)
	if err != nil {
		return err
	}
	m.MinVal = min
	m.MaxVal = max

	log.Printf("Min: %d, Max: %d\n", m.MinVal, m.MaxVal)
	res.Message = "Chunk ricevuto e processato con successo"
	return nil
}

func findMinAndMax(chunk []string) (int, int, error) {
	if len(chunk) == 0 {
		return 0, 0, fmt.Errorf("Chunk vuoto")
	}
	min, err := strconv.Atoi(chunk[0])
	if err != nil {
		return 0, 0, err
	}
	max := min
	for _, str := range chunk[1:] {
		val, err := strconv.Atoi(str)
		if err != nil {
			return 0, 0, err
		}
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
	}
	log.Printf("Min: %d, Max: %d\n", min, max)
	return min, max, nil
}

func (m *Mapper) DistributedValuesToReducers(reducers []Reducer) {
	values := convertToChunkToInt(m.Chunk)

	var wait sync.WaitGroup
	for _, reducer := range reducers {
		wait.Add(1)
		go func(reducer Reducer) {
			defer wait.Done()

			filteredValues := filterValues(values, reducer.Window.Low, reducer.Window.High)
			if len(filteredValues) == 0 {
				return
			}
			client, err := rpc.Dial("tcp", reducer.Address)
			if err != nil {
				log.Printf("Connessione al reducer fallita: %v", err)
			}
			defer client.Close()

			request := MapperToReducer{Values: filteredValues}
			var response ReducerResponse
			err = client.Call("Reducer.AcceptedValues", request, &response)
			if err != nil {
				log.Printf("Errore durante l'invio al reducer %s, %v", reducer.Address, err)
				return
			}
			log.Printf("Reducer %s risposta %s", reducer.Address, response.Message)
		}(reducer)
	}
	wait.Wait()
}

func filterValues(values []int, low int, high int) []int {
	filtered := []int{}
	for _, v := range values {
		if v >= low && v <= high {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func convertToChunkToInt(chunk []string) []int {
	intValues := []int{}
	for _, v := range chunk {
		val, err := strconv.Atoi(v)
		if err == nil {
			intValues = append(intValues, val)
		}
	}
	return intValues
}

func main() {
	mapper := &Mapper{}
	err := rpc.Register(mapper)
	utils.CheckError(err)
	listener, err := net.Listen("tcp", ":0")
	utils.CheckError(err)
	defer listener.Close()

	address := listener.Addr().String()
	log.Printf("Mapper in ascolto sulla porta %s:", address)

	client, err := rpc.Dial("tcp", "localhost:9999")
	utils.CheckError(err)

	defer client.Close()

	registerReq := RegisterMapperRequest{Address: address}
	var registerRes Response
	err = client.Call("Mapper.RegisterMapper", registerReq, &registerRes)
	utils.CheckError(err)
	log.Printf("Mapper registrato nel master", registerRes.Message)

	reducer := []Reducer{
		{ID: 1, Address: fmt.Sprintf(":%d", 50+51), Window: AcceptanceWindow{Low: 0, High: 20}},
		{ID: 2, Address: fmt.Sprintf(":%d", 50+52), Window: AcceptanceWindow{Low: 21, High: 40}},
		{ID: 3, Address: fmt.Sprintf(":%d", 50+53), Window: AcceptanceWindow{Low: 41, High: 60}},
		{ID: 4, Address: fmt.Sprintf(":%d", 50+54), Window: AcceptanceWindow{Low: 61, High: 80}},
		{ID: 5, Address: fmt.Sprintf(":%d", 50+55), Window: AcceptanceWindow{Low: 81, High: 100}},
	}
	//reducer = new(Reducer)
	mapper.DistributedValuesToReducers(reducer)
	//serviamo le richieste in arrivo
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
