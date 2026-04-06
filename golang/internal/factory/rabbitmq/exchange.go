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
	stopCh      chan struct{}
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
		_ = ch.Close()
		_ = conn.Close()
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
	queue, err := e.declareQueue()
	if err != nil {
		return err
	}

	if err := e.bindQueueToKeys(queue); err != nil {
		return err
	}

	return e.consumeMessages(queue, callbackFunc)
}

func (e *exchangeMiddleware) StopConsuming() {
	if e.stopCh != nil {
		close(e.stopCh)
	}
}

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

func (e *exchangeMiddleware) declareQueue() (amqp.Queue, error) {
	q, err := e.channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, m.ErrMessageMiddlewareMessage
	}
	return q, nil
}

func (e *exchangeMiddleware) bindQueueToKeys(queue amqp.Queue) error {
	for _, key := range e.routingKeys {
		if err := e.channel.QueueBind(
			queue.Name,
			key,
			e.exchange,
			false,
			nil,
		); err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *exchangeMiddleware) consumeMessages(
	queue amqp.Queue,
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
	msgs, err := e.channel.Consume(
		queue.Name,
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

	e.stopCh = make(chan struct{})

	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return
				}
				callbackFunc(
					m.Message{Body: string(d.Body)},
					func() { _ = d.Ack(false) },
					func() { _ = d.Nack(false, true) },
				)
			case <-e.stopCh:
				return
			}
		}
	}()

	return nil
}
