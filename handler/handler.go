package handler

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	"golang.org/x/net/context"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	proto "github.com/unicok/chat-srv/proto/chat"
	"github.com/unicok/misc/log"
)

const (
	SERVICE = "[CHAT]"
)

const (
	defaultBoltDBFile = "/data/CHAT.DAT"
	BoltDBBucket      = "EPS"
	PendingSize       = 65536
	CheckInterval     = time.Minute
)

var (
	OK                 = &proto.Nil{}
	ErrorAlreadyExists = errors.New("id already exists")
	ErrorNotExists     = errors.New("id not exists")
	BoltDBFile         = defaultBoltDBFile
	MaxQueueSize       = 128 // num of message kept
)

// Endpoint definition
type EndPoint struct {
	inbox []proto.Message
	ps    *PubSub
	sync.Mutex
}

// NewEndPoint create a new endpoint
func NewEndPoint() *EndPoint {
	u := &EndPoint{}
	u.ps = &PubSub{}
	u.ps.init()
	return u
}

// Push a message to this Endpoint
func (ep *EndPoint) Push(msg *proto.Message) {
	ep.Lock()
	defer ep.Unlock()
	if len(ep.inbox) > MaxQueueSize {
		ep.inbox = append(ep.inbox[1:], *msg)
	} else {
		ep.inbox = append(ep.inbox, *msg)
	}
}

// Read all messages from this Endpoint
func (ep *EndPoint) Read() []proto.Message {
	ep.Lock()
	defer ep.Unlock()
	return append([]proto.Message(nil), ep.inbox...)
}

// server definition
type Chat struct {
	eps     map[uint64]*EndPoint
	pending chan uint64 //dirty id pending
	sync.RWMutex
}

func (s *Chat) Init() {
	s.eps = make(map[uint64]*EndPoint)
	s.pending = make(chan uint64, PendingSize)

	log.Info("bolt db file specified:", BoltDBFile)
	s.restore()
	go s.persistenceTask()
}

func (s *Chat) readEP(id uint64) *EndPoint {
	s.RLock()
	defer s.RUnlock()
	return s.eps[id]
}

func (s *Chat) Subscribe(ctx context.Context, p *proto.Id, stream proto.Chat_SubscribeStream) error {
	// read endpoint
	ep := s.readEP(p.Id)
	if ep == nil {
		log.Errorf("cannot find endpoint %v when Subscribe", p.Id)
		return ErrorNotExists
	}

	// send history chat messages
	msgs := ep.Read()
	for k := range msgs {
		if err := stream.Send(&msgs[k]); err != nil {
			return nil
		}
	}

	// create subsciber
	e := make(chan error, 1)
	var once sync.Once
	f := NewSubscriber(func(msg *proto.Message) {
		if err := stream.Send(msg); err != nil {
			once.Do(func() { // protect for channel blocking
				e <- err
			})
		}
	})

	// subscibe to the endpoint
	log.Debugf("subscribe to :%v", p.Id)
	ep.ps.Sub(f)
	defer func() {
		ep.ps.Unsub(f)
		log.Debugf("unsubscribe from :%v", p.Id)
	}()

	// client send cancel to stop receiving, see service_test.go for example
	select {
	// case <-stream.Context().Done():
	case <-e:
		log.Error(e)
	}
	return nil
}

func (s *Chat) Send(ctx context.Context, msg *proto.Message, rsp *proto.Nil) error {
	ep := s.readEP(msg.Id)
	if ep == nil {
		log.Errorf("cannot find endpoint %v when Send", msg.Id)
		return ErrorNotExists
	}

	ep.ps.Pub(msg)
	ep.Push(msg)
	s.pending <- msg.Id
	rsp = OK
	return nil
}

func (s *Chat) Reg(ctx context.Context, p *proto.Id, rsp *proto.Nil) error {
	s.Lock()
	defer s.Unlock()
	ep := s.eps[p.Id]
	if ep != nil {
		log.Errorf("id already exists:%v when Reg", p.Id)
		return ErrorAlreadyExists
	}

	s.eps[p.Id] = NewEndPoint()
	log.Debug("eps size:", len(s.eps))
	s.pending <- p.Id
	rsp = OK
	return nil
}

// persistenceTask persistence endpoints into db
func (s *Chat) persistenceTask() {
	timer := time.After(CheckInterval)
	db := s.openDB()
	changes := make(map[uint64]bool)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	for {
		select {
		case key := <-s.pending:
			changes[key] = true
		case <-timer:
			s.dump(db, changes)
			if len(changes) > 0 {
				log.Infof("perisisted %v endpoints:", len(changes))
			}
			changes = make(map[uint64]bool)
			timer = time.After(CheckInterval)
		case nr := <-sig:
			s.dump(db, changes)
			db.Close()
			log.Info(nr)
			os.Exit(0)
		}
	}
}

func (s *Chat) openDB() *bolt.DB {
	db, err := bolt.Open(BoltDBFile, 0600, nil)
	if err != nil {
		log.Panic(err)
		os.Exit(-1)
	}
	// create bulket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BoltDBBucket))
		if err != nil {
			log.Panicf("create bucket: %s", err)
			os.Exit(-1)
		}
		return nil
	})
	return db
}

func (s *Chat) dump(db *bolt.DB, changes map[uint64]bool) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BoltDBBucket))
		for k := range changes {
			ep := s.readEP(k)
			if ep == nil {
				log.Errorf("cannot find endpoint %v when dump", k)
				continue
			}

			// serialization and save
			bin, err := msgpack.Marshal(ep.Read())
			if err != nil {
				log.Error("cannot marshal:", err)
				continue
			}

			err = b.Put([]byte(fmt.Sprint(k)), bin)
			if err != nil {
				log.Error(err)
				continue
			}
		}
		return nil
	})
}

func (s *Chat) restore() {
	// restore data from db file
	db := s.openDB()
	defer db.Close()
	count := 0
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BoltDBBucket))
		b.ForEach(func(k, v []byte) error {
			var msg []proto.Message
			err := msgpack.Unmarshal(v, &msg)
			if err != nil {
				log.Error("unmarshal chat msg corrupted:", err)
				os.Exit(-1)
			}
			id, err := strconv.ParseUint(string(k), 0, 64)
			if err != nil {
				log.Error("conv chat id corrupted:", err)
				os.Exit(-1)
			}
			ep := NewEndPoint()
			ep.inbox = msg
			s.eps[id] = ep
			count++
			return nil
		})
		return nil
	})

	log.Infof("restored %v chats", count)
}
