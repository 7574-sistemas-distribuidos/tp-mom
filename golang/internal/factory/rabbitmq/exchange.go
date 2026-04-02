package rabbitmq

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchange    string
	routingKeys []string
}

func NewExchangeMiddleware(
	exchangeName string,
	keys []string,
	settings m.ConnSettings,
) (m.Middleware, error) {
	conn, ch, err := connect(settings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &exchangeMiddleware{
		conn:        conn,
		channel:     ch,
		exchange:    exchangeName,
		routingKeys: keys,
	}, nil
}

func (e *exchangeMiddleware) Send(msg m.Message) error {
	for _, key := range e.routingKeys {
		err := e.channel.Publish(
			e.exchange,
			key,
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
	}
	return nil
}

func (e *exchangeMiddleware) StartConsuming(
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
	q, err := e.channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range e.routingKeys {
		err = e.channel.QueueBind(
			q.Name,
			key,
			e.exchange,
			false,
			nil,
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := e.channel.Consume(
		q.Name,
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			callbackFunc(m.Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		}
	}()

	return nil
}

func (e *exchangeMiddleware) StopConsuming() {}

func (e *exchangeMiddleware) Close() error {
	err := e.channel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}

	err = e.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
