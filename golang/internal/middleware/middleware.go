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
	// Se bloquea y hasta publicar un mensaje a la cola o el contexto sea cancelado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Send(ctx context.Context, msg Message) error

	// Se desconecta del MOM al que estaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Consumer interface {
	// Se bloquea y consume mensajes de la queue hasta que el contexto no sea cancelado u ocurra un error.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Consume(ctx context.Context, msgs chan<- Message) error

	// Cancela el contexto del método Consume, desbloqueando el llamado
	StopConsuming()

	// Se desconecta del MOM al que estaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Publisher interface {
	// Se bloquea y hasta publicar un mensaje al topic o el contexto sea cancelado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Publish(ctx context.Context, msg Message) error

	// Se desconecta del MOM al que etaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}

type Subscriber interface {
	// Se bloquea y consume mensajes del topic hasta que el contexto no sea cancelado u ocurra un error.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Consume(ctx context.Context, msgs chan<- Message) error

	// Cancela el contexto del método Consume, desbloqueando el llamado
	StopConsuming()

	// Se desconecta del MOM al que estaba conectado.
	// Si ocurre un error interno que no puede resolverse devuelve ErrClosed.
	Close() error
}
