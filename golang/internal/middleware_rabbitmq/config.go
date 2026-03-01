package middleware_rabbitmq

import "fmt"

type ConnectionConfiguration struct {
	Hostname string
	Port     int
}

func (r ConnectionConfiguration) ConnectionString() string {
	return "amqp://" + r.Hostname + ":" + fmt.Sprint(r.Port)
}
