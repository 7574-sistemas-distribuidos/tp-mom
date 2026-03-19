package tests

import (
	"fmt"
	"slices"
	"testing"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	s "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/solution"
	"github.com/stretchr/testify/assert"
)

func GetQueueMiddleware(queue string) (m.Middleware, error) {
	// TODO: Implementar la función constructora para
	// devolver su implementación del middleware para Queues
	return s.NewRabbitmqQueueMiddleware(queue, GetCredentials())
}

func TestCanConnect(t *testing.T) {
	producer_mw, init_error := GetQueueMiddleware("TestCanConnect")
	assert.NoError(t, init_error)

	close_error := producer_mw.Close()
	assert.NoError(t, close_error)
}

// TODO: Send 3, 6, and 16 msgs in each case
func TestOneToOne(t *testing.T) {

	// Arrange
	queue_name := "TestOneToOne"
	producer_mw, init_error := GetQueueMiddleware(queue_name)
	assert.NoError(t, init_error)

	msgEquals := make(chan bool)

	expected_msg := "Hello World!"

	// Act
	go func() {
		compare_msg := func(msg m.Message) {
			msgEquals <- msg.Body == expected_msg
		}

		consumer_mw, init_err := GetQueueMiddleware(queue_name)
		assert.NoError(t, init_err)

		cons_err := consumer_mw.StartConsuming(compare_msg)
		assert.NoError(t, cons_err)
	}()

	send_error := producer_mw.Send(m.Message{Body: expected_msg})
	assert.NoError(t, send_error)

	isMessageEqual := <-msgEquals

	close_error := producer_mw.Close()
	assert.NoError(t, close_error)

	// Assert
	assert.True(t, isMessageEqual)
}

func TestOneToMany(t *testing.T) {

	// Arrange
	num_of_consumers := 3

	GetExepctedMsg := func(num int) string {
		return "Hello " + fmt.Sprint(num)
	}

	GetQueueName := func(num int) string {
		return "TestOneToMany_" + fmt.Sprint(num)
	}

	// Act
	for i := range num_of_consumers {
		producer_mw, init_err := GetQueueMiddleware(GetQueueName(i))
		assert.NoError(t, init_err)

		send_err := producer_mw.Send(m.Message{Body: GetExepctedMsg(i)})
		assert.NoError(t, send_err)

		close_err := producer_mw.Close()
		assert.NoError(t, close_err)
	}

	msgEquals := make(chan bool)
	for i := range num_of_consumers {
		go func(num int) {
			compare_msg := func(msg m.Message) {
				msgEquals <- msg.Body == GetExepctedMsg(num)
			}

			consumer_mw, init_err := GetQueueMiddleware(GetQueueName(num))
			assert.NoError(t, init_err)

			cons_err := consumer_mw.StartConsuming(compare_msg)
			assert.NoError(t, cons_err)

			close_err := consumer_mw.Close()
			assert.NoError(t, close_err)

		}(i)
	}

	comparison_results := make([]bool, 0)
	for range num_of_consumers {
		a := <-msgEquals
		comparison_results = append(comparison_results, a)
	}

	close(msgEquals)

	// Assert
	for i := range num_of_consumers {
		assert.True(t, comparison_results[i])
	}
}

func TestManyToOne(t *testing.T) {
	// Arrange
	num_of_producers := 3

	GetExepctedMsg := func(num int) string {
		return "Hello From " + fmt.Sprint(num)
	}

	queue_name := "TestManyToOne"

	// Act
	for i := range num_of_producers {
		go func(p_id int) {
			producer_mw, init_err := GetQueueMiddleware(queue_name)
			assert.NoError(t, init_err)

			send_err := producer_mw.Send(m.Message{Body: GetExepctedMsg(i)})
			assert.NoError(t, send_err)

			close_err := producer_mw.Close()
			assert.NoError(t, close_err)
		}(i)
	}

	expectedMsgs := make([]string, 0)
	for i := range num_of_producers {
		expectedMsgs = append(expectedMsgs, GetExepctedMsg(i))
	}

	msgs := make(chan string)
	cb := func(msg m.Message) {
		msgs <- msg.Body
	}

	go func() {
		consumer_mw, init_err := GetQueueMiddleware(queue_name)
		assert.NoError(t, init_err)

		cons_err := consumer_mw.StartConsuming(cb)
		assert.NoError(t, cons_err)

		close_err := consumer_mw.Close()
		assert.NoError(t, close_err)
	}()

	comparison_results := make([]bool, 0)
	for range num_of_producers {
		received_msg := <-msgs
		comparison_results = append(comparison_results, slices.Contains(expectedMsgs, received_msg))
		Remove(expectedMsgs, received_msg) // Does nothing if not present
	}

	close(msgs)

	// Assert
	for i := range num_of_producers {
		assert.True(t, comparison_results[i])
	}
}
