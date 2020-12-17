package main

import (
	"context"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"time"
)

type server struct{}

func (s *server) FetchData(request *messagepb.Request, stream messagepb.MyDataService_FetchDataServer) error {
	fmt.Printf("\nFetchData was invoked, this will now stream data back to client...")
	myData := request.GetData()
	firstName := myData.GetFirstName()
	lastName := myData.GetLastName()
	age := myData.GetAge()
	for i := 0; i < 10; i++ {
		result := "Hello " + firstName + " , " + lastName + " whose age is " + strconv.Itoa(int(age)) + " : counter => " + strconv.Itoa(i)
		responseResult := &messagepb.Response{Result: result}
		_ = stream.Send(responseResult)
		fmt.Printf("\nstream.Send...")
		time.Sleep(1000 * time.Millisecond)
	}
	return nil
}

func (*server) GetSum(ctx context.Context, req *messagepb.SumRequest) (*messagepb.SumResponse, error) {
	fmt.Printf("\nReceived Sum Request : %v", req)
	num1 := req.NumberFirst
	num2 := req.NumberSecond

	total := num1 + num2
	result := &messagepb.SumResponse{SumResult: total}
	return result, nil
}

func main() {
	fmt.Printf("\nserver ...")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen : %v", err)
	}

	myServer := grpc.NewServer()

	messagepb.RegisterMyDataServiceServer(myServer, &server{})
	if err := myServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve : %v", err)
	}
}
