package rabbitmq

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string

	deliveries <-chan amqp.Delivery
	done       chan struct{}
}

func (q *queueMiddleware) Send(msg m.Message) error {
	err := q.channel.Publish(
		"",
		q.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (q *queueMiddleware) StartConsuming(
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
	msgs, err := q.channel.Consume(
		q.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	q.deliveries = msgs
	q.done = make(chan struct{})

	go q.consumeLoop(callbackFunc)

	return nil
}

func (q *queueMiddleware) StopConsuming() {
	if q.done != nil {
		close(q.done)
	}
}

func (q *queueMiddleware) Close() error {
	if q.channel != nil {
		err := q.channel.Close()
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}

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
		_ = ch.Close()
		_ = conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &queueMiddleware{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}

func (q *queueMiddleware) consumeLoop(
	callbackFunc func(msg m.Message, ack func(), nack func()),
) {

	for {
		select {

		case d, ok := <-q.deliveries:
			if !ok {
				return
			}

			q.handleDelivery(d, callbackFunc)

		case <-q.done:
			return
		}
	}
}

func (q *queueMiddleware) handleDelivery(
	d amqp.Delivery,
	callbackFunc func(msg m.Message, ack func(), nack func()),
) {

	message := m.Message{
		Body: string(d.Body),
	}

	ack := func() {
		_ = d.Ack(false)
	}

	nack := func() {
		_ = d.Nack(false, true)
	}

	callbackFunc(message, ack, nack)
}
