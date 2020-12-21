package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/giridharmb/grpc-messagepb"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var mutex = &sync.Mutex{}

type server struct{}

func writeFileV1(name string, byteArray []byte) error {
	err := ioutil.WriteFile(name, byteArray, 0644)
	if err != nil {
		fmt.Printf("\n(ERROR) : could not write to file (%v) : %v", name, err.Error())
		return err
	}

	fmt.Printf("\nsuccessfully wrote to file : (%v)", name)
	return nil
}

func writeFileV2(name string, byteArray []byte) error {
	f, err := os.Create(name)
	if err != nil {
		fmt.Printf("\n(ERROR) : could not create file (%v) : %v", name, err.Error())
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	totalBytesWritten, err := f.Write(byteArray)
	if err != nil {
		fmt.Printf("\n(ERROR) : could not write to file (%v) : %v", name, err.Error())
		return err
	}
	fmt.Printf("\ntotalBytesWritten : %v", totalBytesWritten)
	defer func() {
		_ = f.Sync()
	}()
	return nil
}

func (s *server) BDTransfer(stream messagepb.MyDataService_BDTransferServer) error {

	fmt.Printf("\nBDTransfer was invoked, this is a bi-directional stream...")

	completeData := make([]byte, 0)

	fileName := ""

	waitChannel := make(chan struct{})

	readBytes := make([]byte, 0)

	totalBytesRead := 0

	go func() {
		fmt.Printf("\nGoing to read from stream...")
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				fmt.Printf("\nClosing the channel...")
				close(waitChannel)
				fmt.Printf("\nClosed the channel.")
				break
			}
			if err != nil {
				fmt.Printf("\n(ERROR) : could not read from client stream : %v", err)
				break
			}

			readBytes = req.GetData()

			fileName = req.GetFileName()

			totalFileSize := req.GetBytesTotalSize()

			for _, data := range readBytes {
				completeData = append(completeData, data)
			}

			totalBytesRead = totalBytesRead + len(readBytes)

			percentComplete := float32(totalBytesRead) / float32(totalFileSize) * 100.0

			sendErr := stream.Send(&messagepb.BDTransferResponse{PercentComplete: percentComplete})
			if sendErr != nil {
				fmt.Printf("\nerror in sending back to client : %v", sendErr)
			}
		}
	}()

	<-waitChannel

	fmt.Printf("\nBDTransfer : io.EOF received.")

	_ = writeFileV2(fileName, completeData)

	return nil
}

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

	var port string
	flag.StringVar(&port, "port", "50051", "name of port on which server should listen")

	flag.Parse()

	hostAndPort := fmt.Sprintf("0.0.0.0:%v", port)

	lis, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		log.Fatalf("\nFailed to listen on port (%v) : %v", port, err.Error())
	}

	fmt.Printf("\nRPC Server Is Listening on (%v) ...\n", hostAndPort)

	myServer := grpc.NewServer()

	messagepb.RegisterMyDataServiceServer(myServer, &server{})
	if err := myServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve : %v", err)
	}
}
