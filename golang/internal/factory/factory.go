package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMw(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}

func CreateExchangeMw(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
