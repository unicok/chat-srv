package main

import (
	_ "lib/statsd-pprof"
	"time"

	"github.com/micro/cli"
	micro "github.com/micro/go-micro"

	"github.com/unicok/chat-srv/handler"
	proto "github.com/unicok/chat-srv/proto/chat"
	"github.com/unicok/misc/log"
)

func main() {
	service := micro.NewService(
		micro.Name("com.unicok.srv.chat"),
		micro.Version("latest"),
		micro.RegisterTTL(time.Minute),
		micro.RegisterInterval(time.Second*30),

		micro.Flags(
			cli.StringFlag{
				Name:   "bolt_db_file",
				EnvVar: "BOLT_DB_FILE",
				Usage:  "Bolt db file path for the message store",
			},

			cli.IntFlag{
				Name:   "max_queue_size",
				EnvVar: "MAX_QUEUE_SIZE",
				Usage:  "Num of message kept",
			},
		),

		micro.Action(func(c *cli.Context) {
			if len(c.String("bolt_db_file")) > 0 {
				handler.BoltDBFile = c.String("bolt_db_file")
			}

			if len(c.String("max_queue_size")) > 0 {
				handler.MaxQueueSize = c.Int("max_queue_size")
			}
		}),
	)

	// Register Subscribers
	// if err := service.Server().Subscribe(
	// 	service.Server().NewSubscriber(
	// 		"topic.com.unicok.srv.chat",

	// 	)
	// ); err != nil {
	// 	log.Fatal(err)
	// }

	service.Init()
	chat := new(handler.Chat)
	chat.Init()

	proto.RegisterChatHandler(service.Server(), chat)

	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
