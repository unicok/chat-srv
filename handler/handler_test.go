package handler

import (
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	proto "github.com/unicok/chat-srv/proto/chat"
)

const (
	address = "127.0.0.1:50008"
)

var (
	conn *grpc.ClientConn
	err  error
)

func TestChat(t *testing.T) {
	// Setup a connection to the server
	conn, err = grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}

	c := proto.NewChatClient(conn)

	// Contact the server and print out its response.
	_, err = c.Reg(context.Background(), &pb.Chat_Id{Id: 1})
	if err != nil {
		t.Logf("could not query: %v", err)
	}

	const COUNT = 10
	go send(&proto.Message{Id: 1, Body: []byte("hello")}, COUNT, t)
	go recv(&proto.Id{Id: 1}, COUNT, t)
	go recv(&proto.Id{Id: 1}, COUNT, t)
	time.Sleep(3 * time.Second)
}

func send(m *proto.Message, count int, t *testing.T) {
	c := proto.NewChatClient(conn)
	for {
		if count == 0 {
			return
		}
		_, err := c.Send(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("send:", m)
		count--
	}
}

func recv(id *proto.Id, count int, t *testing.T) {
	c := proto.NewChatServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.Subscribe(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if count == 0 {
			return
		}
		msg, err := stream.Recv()
		if err != nil {
			t.Log(err)
			return
		}
		println("recv:", count)
		t.Log("recv:", msg)
		count--
		cancel() // recv should continue until error
	}
}
