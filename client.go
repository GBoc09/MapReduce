package main

import (
	"GoLandFiles/utils"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

/** il client deve scrivere a schermo lo schema finale dei valori in ordine crescente */

func main() {
	var dataset []string
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	for i := 0; i < 50; i++ {
		num := r.Intn(100) + 1
		dataset = append(dataset, fmt.Sprintf("%d", num))
	}
	log.Printf("Dataset generato %v", dataset)
	addr := "localhost:" + "9999"        // ascolto server
	client, err := rpc.Dial("tcp", addr) // client
	defer client.Close()
	utils.CheckError(err)

	input := utils.DatasetInput{Data: dataset}
	var output utils.DatasetOutput

	err = client.Call("Master.Master", input, &output)
	utils.CheckError(err)
	log.Println("Risultato server: %s ", output.Message)
}
