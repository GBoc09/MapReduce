package utils

import "log"

type WorkerArgs struct {
	Job          map[int32]int32
	ToDo         []int32
	WorkerID     int
	WorkerRanges map[int][]int32
}
type WorkerReply struct {
	Ack  string
	Data map[int32]int32
}
type ReduceReply struct {
	Ack string
}
type ReduceArgs struct {
	WorkerID     int
	WorkerRanges map[int][]int32
}
type WorkerData struct {
	WorkerID int
	Data     map[int32]int32
}

type DatasetInput struct {
	Data []int32
}
type ClientMessage struct {
	Message string
}
type ClientResponse struct {
	Data []int32
	Ack  string
}
type DatasetOutput struct {
	Ack       string
	FinalData []int32
}

func CheckError(err error) {
	// if an error is returned, print it to the console and exit
	if err != nil {
		log.Fatal(err)
	}
}
