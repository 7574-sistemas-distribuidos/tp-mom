package rabbitmq

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

type exchangeMiddleware struct{}

func NewExchangeMiddleware(exchangeName string, keys []string, settings m.ConnSettings) (m.Middleware, error) {
	return &exchangeMiddleware{}, nil
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
	return nil
}
