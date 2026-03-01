package middleware

type Message struct {
	Body string
}

type Middleware interface {
	StartConsuming(cb func(msg Message)) (err error)
	StopConsuming()
	Send(msg Message) (err error)
	Close() error
}
