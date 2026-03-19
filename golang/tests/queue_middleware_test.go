package tests

import (
	"testing"

	f "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/stretchr/testify/assert"
)

func GetQueueMiddleware(queue string) (m.Middleware, error) {
	return f.CreateQueueMw(queue, GetConnectionDetails())
}

type QueueProdSettings struct {
	MessagesByQueue map[string][]string
}

type QueueConsSettings struct {
	QueueName string
}

func TestCanConnect(t *testing.T) {
	producer_mw, init_error := GetQueueMiddleware("TestCanConnect")
	assert.NoError(t, init_error)

	close_error := producer_mw.Close()
	assert.NoError(t, close_error)
}

func TestOneToOne(t *testing.T) {

	// Arrange
	p_settings := []QueueProdSettings{
		{MessagesByQueue: map[string][]string{
			"TestOneToOne": {
				"JavaScript",
				"Python",
				"Java",
				"C",
				"C++",
				"C#",
				"TypeScript",
				"Ruby",
				"Go",
				"Rust",
				"Swift",
				"Kotlin",
				"PHP",
				"SQL",
				"Assembly",
			},
		}},
	}

	cons_settings := []QueueConsSettings{
		{QueueName: "TestOneToOne"},
	}

	DoTestQueue(t, p_settings, cons_settings)
}

func TestOneToMany(t *testing.T) {

	// Arrange
	p_settings := []QueueProdSettings{
		{MessagesByQueue: map[string][]string{
			"TestOneToMany": {
				"Buenos Aires",
				"Córdoba",
				"Rosario",
				"Mendoza",
				"San Miguel de Tucumán",
				"La Plata",
				"Mar del Plata",
				"Salta",
				"Santa Fe",
				"San Juan",
				"Resistencia",
				"Neuquén",
				"San Salvador de Jujuy",
				"Posadas",
				"Corrientes",
				"Bahía Blanca",
				"San Luis",
				"Bariloche",
				"Ushuaia",
				"Río Gallegos",
			},
		}},
	}

	cons_settings := []QueueConsSettings{
		{QueueName: "TestOneToMany"},
		{QueueName: "TestOneToMany"},
		{QueueName: "TestOneToMany"},
		{QueueName: "TestOneToMany"},
		{QueueName: "TestOneToMany"},
	}

	DoTestQueue(t, p_settings, cons_settings)
}

func TestManyToOne(t *testing.T) {

	// Arrange
	p_settings := []QueueProdSettings{
		{MessagesByQueue: map[string][]string{
			"TestManyToOne": {"Buenos Aires", "Córdoba", "Rosario", "Mendoza", "San Miguel de Tucumán"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToOne": {"La Plata", "Mar del Plata", "Salta", "Santa Fe", "San Juan"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToOne": {"Resistencia", "Neuquén", "San Salvador de Jujuy", "Posadas", "Corrientes"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToOne": {"Bahía Blanca", "San Luis", "Bariloche", "Ushuaia", "Río Gallegos"},
		}},
	}

	cons_settings := []QueueConsSettings{
		{QueueName: "TestManyToOne"},
	}

	DoTestQueue(t, p_settings, cons_settings)
}

func TestManyToMany(t *testing.T) {

	// Arrange
	p_settings := []QueueProdSettings{
		{MessagesByQueue: map[string][]string{
			"TestManyToMany": {"Buenos Aires", "Córdoba", "Rosario", "Mendoza", "San Miguel de Tucumán"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToMany": {"La Plata", "Mar del Plata", "Salta", "Santa Fe", "San Juan"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToMany": {"Resistencia", "Neuquén", "San Salvador de Jujuy", "Posadas", "Corrientes"},
		}},
		{MessagesByQueue: map[string][]string{
			"TestManyToMany": {"Bahía Blanca", "San Luis", "Bariloche", "Ushuaia", "Río Gallegos"},
		}},
	}

	cons_settings := []QueueConsSettings{
		{QueueName: "TestManyToMany"},
		{QueueName: "TestManyToMany"},
		{QueueName: "TestManyToMany"},
		{QueueName: "TestManyToMany"},
	}

	DoTestQueue(t, p_settings, cons_settings)
}

func DoTestQueue(t *testing.T, producers_settings []QueueProdSettings, consumer_settings []QueueConsSettings) {

	// Arrange
	msgs_fan_in := make(chan string)
	prod_by_queue := make(map[string]m.Middleware)
	n_cons_by_queue := make(map[string]int)
	for _, p_settings := range producers_settings {
		for q_name := range p_settings.MessagesByQueue {
			mw, err := GetQueueMiddleware(q_name)
			assert.NoError(t, err)
			prod_by_queue[q_name] = mw

			n_cons_by_queue[q_name] = 0
		}
	}

	consumers := make([]m.Middleware, 0)
	for _, c_settings := range consumer_settings {
		mw, err := GetQueueMiddleware(c_settings.QueueName)
		assert.NoError(t, err)
		consumers = append(consumers, mw)
		n_cons_by_queue[c_settings.QueueName] += 1

		go mw.StartConsuming(func(msg m.Message) { msgs_fan_in <- msg.Body })
	}

	// Act
	for _, p_settings := range producers_settings {
		for q_name, msgs := range p_settings.MessagesByQueue {
			for _, msg := range msgs {
				prod_by_queue[q_name].Send(m.Message{Body: msg})
			}
		}
	}

	expected_deliveries := make([]string, 0)
	for _, p_settings := range producers_settings {
		for _, msgs := range p_settings.MessagesByQueue {
			expected_deliveries = append(expected_deliveries, msgs...)
		}
	}

	comparison_results := make([]bool, 0)

	deliveries := len(expected_deliveries)

	for range deliveries {
		Remove(expected_deliveries, <-msgs_fan_in) // Does nothing if not present
	}
	close(msgs_fan_in)

	// Assert
	assert.Empty(t, comparison_results)

	for _, p := range prod_by_queue {
		close_err := p.Close()
		assert.NoError(t, close_err)
	}

	for _, c := range consumers {
		c.StopConsuming()
		close_err := c.Close()
		assert.NoError(t, close_err)
	}
}
