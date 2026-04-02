package factory

import (
	r "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/rabbitmq"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return r.NewQueueMiddleware(
		queueName,
		connectionSettings,
	)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return r.NewExchangeMiddleware(
		exchange,
		keys,
		connectionSettings,
	)
}
