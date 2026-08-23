package middleware

import (
	"context"
	"errors"
)

var (
	ErrMessage      = errors.New("message middleware: message error")
	ErrDisconnected = errors.New("message middleware: disconnected")
	ErrClosed       = errors.New("message middleware: close error")
)

type ConnSettings struct {
	Hostname string
	Port     int
}

type Connection interface {
	Producer(queue string) (Producer, error)
	Consumer(queue string) (Consumer, error)
	Publisher(topic string) (Publisher, error)
	Subscriber(queue string) (Subscriber, error)
	Close() error
}

type Message struct {
	Body []byte
	Ack  func() error
	Nack func() error
}

type Producer interface {
	// Se envía un mensaje a la cola
	Send(ctx context.Context, msg Message) error

	// Se desconecta del MOM al que estaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Consumer interface {
	// Se bloquea y consume mensajes de la queue o topic establecido
	Consume(ctx context.Context, msgs chan<- Message) error

	// Deja de consumir
	StopConsuming()

	// Se desconecta del MOM al que estaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Publisher interface {
	// Se publica un mensaje al topic
	Publish(ctx context.Context, msg Message) error

	// Se desconecta del Mom al que etaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Subscriber interface {
	// Se bloquea y consume mensajes de la queue o topic establecido
	Consume(ctx context.Context, msgs chan<- Message) error

	// Deja de consumir
	StopConsuming()

	// Se desconecta del Mom al que etaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}
