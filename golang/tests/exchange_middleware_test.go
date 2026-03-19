package tests

import (
	"fmt"
	"slices"
	"testing"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	s "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/solution"
	"github.com/stretchr/testify/assert"
)

func GetExchangeMiddleware(exchange string, keys []string) (m.Middleware, error) {
	// TODO: Implementar la función constructora para
	// devolver su implementación del middleware para Exchanges
	return s.NewRabbitmqExchangeMiddleware(exchange, keys, GetCredentials())
}

var w_opts = GetWaitOptions()

func TestCanConnectExchange(t *testing.T) {
	producer_mw, init_error := GetExchangeMiddleware("test_exchange", []string{"TestCanConnect"})
	assert.NoError(t, init_error)

	close_error := producer_mw.Close()
	assert.NoError(t, close_error)
}

func TestOneToOneExchange(t *testing.T) {

	// Arrange
	expected_msg := "Hello World!"
	exchange := "test_exchange"
	keys := []string{"TestOneToOneExchange"}

	producer_mw, init_error := GetExchangeMiddleware(exchange, keys)
	assert.NoError(t, init_error)

	msgEquals := make(chan bool)

	// Act
	consumer_mw, init_err := GetExchangeMiddleware(exchange, keys)
	assert.NoError(t, init_err)
	go func() {
		cons_err := consumer_mw.StartConsuming(func(msg m.Message) {
			msgEquals <- msg.Body == expected_msg
		})
		assert.NoError(t, cons_err)

	}()

	wait_err := WaitForExchangeBindings(exchange, keys[0], 1, w_opts)
	assert.NoError(t, wait_err)

	send_error := producer_mw.Send(m.Message{Body: expected_msg})
	assert.NoError(t, send_error)
	isMessageEqual := <-msgEquals
	close_error := producer_mw.Close()
	assert.NoError(t, close_error)
	close_error = consumer_mw.Close()
	assert.NoError(t, close_error)

	// Assert
	assert.True(t, isMessageEqual)
}

func TestOneToManyExchange(t *testing.T) {
	// Arrange
	expected_msg := "Hello World!"
	exchange := "test_exchange"
	keys := []string{"TestOneToManyExchange"}
	num_of_consumers := 3
	msgs_per_producer := 8

	// Act
	consumer_mws := make([]m.Middleware, 0)
	msgEquals := make(chan bool, num_of_consumers)
	for i := range num_of_consumers {
		go func(num int) {
			consumer_mw, init_err := GetExchangeMiddleware(exchange, keys)
			assert.NoError(t, init_err)
			consumer_mws = append(consumer_mws, consumer_mw)

			cons_err := consumer_mw.StartConsuming(func(msg m.Message) {
				msgEquals <- msg.Body == expected_msg
			})
			assert.NoError(t, cons_err)
		}(i)
	}

	wait_err := WaitForExchangeBindings(exchange, keys[0], num_of_consumers, w_opts)
	assert.NoError(t, wait_err)

	producer_mw, init_err := GetExchangeMiddleware(exchange, keys)
	assert.NoError(t, init_err)

	for range msgs_per_producer {
		send_err := producer_mw.Send(m.Message{Body: expected_msg})
		assert.NoError(t, send_err)
	}

	close_err := producer_mw.Close()
	assert.NoError(t, close_err)

	comparison_results := make([]bool, 0)
	for range num_of_consumers * msgs_per_producer {
		comparison_results = append(comparison_results, <-msgEquals)
	}

	// Assert
	assert.Equal(t, num_of_consumers*msgs_per_producer, len(comparison_results))
	for i := range num_of_consumers * msgs_per_producer {
		assert.True(t, comparison_results[i])
	}

	for i := range num_of_consumers {
		close_err = consumer_mws[i].Close()
		assert.NoError(t, close_err)
	}

	close(msgEquals)
}

func TestManyToOneExchange(t *testing.T) {
	// Arrange
	num_of_producers := 3
	msgs_per_producer := 8
	exchange := "test_exchange"
	keys := []string{"TestManyToOneExchange"}

	GetExepctedMsg := func(num int) string {
		return "Hello From " + fmt.Sprint(num)
	}

	// Act
	consumer_mw, init_err := GetExchangeMiddleware(exchange, keys)
	assert.NoError(t, init_err)
	msgs := make(chan string)
	go func() {
		cons_err := consumer_mw.StartConsuming(func(msg m.Message) {
			msgs <- msg.Body
		})
		assert.NoError(t, cons_err)
	}()

	wait_err := WaitForExchangeBindings(exchange, keys[0], 1, w_opts)
	assert.NoError(t, wait_err)

	for i := range num_of_producers {
		go func(p_id int) {
			producer_mw, init_err := GetExchangeMiddleware(exchange, keys)
			assert.NoError(t, init_err)

			for range msgs_per_producer {
				send_err := producer_mw.Send(m.Message{Body: GetExepctedMsg(i)})
				assert.NoError(t, send_err)
			}
			close_err := producer_mw.Close()
			assert.NoError(t, close_err)
		}(i)
	}

	expectedMsgs := make([]string, 0)
	for i := range num_of_producers {
		for range msgs_per_producer {
			expectedMsgs = append(expectedMsgs, GetExepctedMsg(i))
		}
	}

	comparison_results := make([]bool, 0)
	for range num_of_producers * msgs_per_producer {
		received_msg := <-msgs
		comparison_results = append(comparison_results, slices.Contains(expectedMsgs, received_msg))
		Remove(expectedMsgs, received_msg) // Does nothing if not present
	}

	close_err := consumer_mw.Close()
	assert.NoError(t, close_err)

	// Assert
	for i := range num_of_producers {
		assert.True(t, comparison_results[i])
	}
	close(msgs)
}
