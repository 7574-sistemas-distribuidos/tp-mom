package rabbitmq

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

func (q *queueMiddleware) Send(msg m.Message) error {
	return nil
}

func (q *queueMiddleware) StartConsuming(
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
	return nil
}

func (q *queueMiddleware) StopConsuming() {}

func (q *queueMiddleware) Close() error {
	if q.conn != nil {
		err := q.conn.Close()
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}

	return nil
}

func NewQueueMiddleware(queueName string, settings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := connect(settings)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &queueMiddleware{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}
