package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

type server struct{}

func (s *server) BDStream(stream messagepb.MyDataService_BDStreamServer) error {
	fmt.Printf("\nBDStream was invoked, this is a bi-directional stream...")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Printf("\nerror while reading from client stream : %v", err)
			return err
		}
		firstName := req.GetLastName()
		lastName := req.GetLastName()
		combinedString := fmt.Sprintf("%v , %v", lastName, firstName)
		dataBytes := []byte(combinedString)
		md5Hash := fmt.Sprintf("%x", md5.Sum(dataBytes))
		sendErr := stream.Send(&messagepb.BDStreamMessageResponse{Hash: md5Hash})
		if sendErr != nil {
			fmt.Printf("\nerror in sending back to client : %v", sendErr)
			return sendErr
		}
	}
}

func (s *server) ClientStream(stream messagepb.MyDataService_ClientStreamServer) error {
	fmt.Printf("\nClientStream was invoked, this will now read the stream of data from client...")
	result := "DONE_FROM_SERVER"

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// we have finished reading the client stream

			myResponse := &messagepb.DataResponseClientStream{String_: result}

			return stream.SendAndClose(myResponse)
		}
		if err != nil {
			log.Fatalf("\nerror while reading the client stream : %v", err)
		}
		myRandomString := req.GetRandomString()
		myIndex := req.GetIndex()

		fmt.Printf("\nmyRandomString => %v", myRandomString)
		fmt.Printf("\nmyIndex => %v", myIndex)
	}
}

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
