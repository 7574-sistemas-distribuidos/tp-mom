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
	return nil
}

func (e *exchangeMiddleware) StartConsuming(
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
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
