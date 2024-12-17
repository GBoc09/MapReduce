package main

import (
	"GoLandFiles/utils"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

type Client struct{}

/** il client deve scrivere a schermo lo schema finale dei valori in ordine crescente */

func main() {
	var dataset []int32
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	for i := 0; i < 100; i++ {
		num := int32(r.Intn(100) + 1)
		dataset = append(dataset, num)
	}
	log.Printf("Dataset generato %v", dataset)
	addr := "localhost:" + "9999"        // ascolto server
	client, err := rpc.Dial("tcp", addr) // client
	defer client.Close()
	utils.CheckError(err)

	input := &utils.DatasetInput{Data: dataset}
	output := &utils.DatasetOutput{}

	err = client.Call("Master.MasterReceiveData", input, output)
	utils.CheckError(err)
	log.Println("Risultato finale: %s ", output.FinalData)
}
