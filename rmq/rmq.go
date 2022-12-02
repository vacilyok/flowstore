package rmq

import (
	cfg "flowstore/config"
	"fmt"
	"log"

	amqp "github.com/amqp"
)

func ConnectToRabbitMq() (<-chan amqp.Delivery, error) {
	conn_str := fmt.Sprintf("amqp://%s:%s@%s:%d", cfg.Config.Rabbituser, cfg.Config.Rabbitpass, cfg.Config.RabbitHost, cfg.Config.RabbitPort)
	connectRabbitMQ, err := amqp.Dial(conn_str)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	queue, err := channelRabbitMQ.QueueDeclare(cfg.Config.RabbitQueue, true, true, false, false, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	err = channelRabbitMQ.QueueBind(queue.Name, "", cfg.Config.RabbitExchange, false, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	deliveries, err := channelRabbitMQ.Consume(queue.Name, "", true, false, false, false, nil)
	return deliveries, err

}
