package main

import (
	_ "flowstore/config"
	db "flowstore/db/clickhouse"
	fmsgs "flowstore/pb"
	"flowstore/rmq"
	"log"

	clickhouse "github.com/ClickHouse/clickhouse-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	var err error
	db.DbConn, err = db.Connection()
	if err != nil {
		log.Println(err)
		return
	}
	if ok := db.DbInit(); ok != nil {
		log.Println("Database init is fail")
		return
	}
	log.Println("Connect to database successful")
	forever := make(chan bool)
	messages, err := rmq.ConnectToRabbitMq()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Connect to broker successful")

	dbRowChan := make(chan clickhouse.Row)
	defer close(dbRowChan)
	go db.Collect(dbRowChan)
	for message := range messages {
		flowMess := &fmsgs.FlowMessage{}
		proto.Unmarshal(message.Body, flowMess)
		db.Decompose(flowMess, dbRowChan)
	}
	<-forever

}
