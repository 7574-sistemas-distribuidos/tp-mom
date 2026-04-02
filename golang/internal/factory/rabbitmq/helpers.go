package rabbitmq

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(settings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf(
		"amqp://guest:guest@%s:%d/",
		settings.Hostname,
		settings.Port,
	)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}

	return conn, ch, nil
}
