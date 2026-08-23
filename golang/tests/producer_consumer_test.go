package tests

import (
	"testing"

	f "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/stretchr/testify/assert"
)

type ProducerSettings struct {
	QueueName string
	Messages  []string
}

type ConsumerSettings struct {
	QueueName string
}

func TestCanConnectProducerConsumer(t *testing.T) {
	const queueName = "queue"
	conn, err := f.GetConnection(GetConnectionDetails())
	assert.NoError(t, err)

	_, err = conn.Producer(queueName)
	assert.NoError(t, err)

	_, err = conn.Consumer(queueName)
	assert.NoError(t, err)

	err = conn.Close()
	assert.NoError(t, err)
}

func TestOneProducerOneConsumer(t *testing.T) {
	// Arrange
	const queueName = "TestOneProducerOneConsumer"
	producersDeclaration := []ProducerSettings{
		{QueueName: queueName, Messages: []string{
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
		}},
	}

	consumersDeclaration := []ConsumerSettings{
		{QueueName: queueName},
	}

	DoTestProducerConsumer(t, producersDeclaration, consumersDeclaration)
}

func TestWorkingQueue(t *testing.T) {

	// Arrange
	const queueName = "TestWorkingQueue"
	producersDeclaration := []ProducerSettings{
		{QueueName: queueName, Messages: []string{
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
		}},
	}

	consumersDeclaration := []ConsumerSettings{
		{QueueName: queueName},
		{QueueName: queueName},
		{QueueName: queueName},
		{QueueName: queueName},
		{QueueName: queueName},
	}

	DoTestProducerConsumer(t, producersDeclaration, consumersDeclaration)
}

func TestManyProducersOneConsumer(t *testing.T) {

	// Arrange
	const queueName = "TestManyProducersOneConsumer"
	producersDeclaration := []ProducerSettings{
		{QueueName: queueName, Messages: []string{"Empanadas", "Asado", "Locro", "Dulce de leche", "Alfajores"}},
		{QueueName: queueName, Messages: []string{"Milanesa", "Choripán", "Humita", "Tamales", "Provoleta"}},
		{QueueName: queueName, Messages: []string{"Carbonada", "Matambre arrollado", "Pastelitos", "Chocotorta", "Facturas"}},
		{QueueName: queueName, Messages: []string{"Fainá", "Tortas fritas", "Sorrentinos", "Guiso de lentejas", "Puchero"}},
	}

	consumersDeclaration := []ConsumerSettings{
		{QueueName: queueName},
	}

	DoTestProducerConsumer(t, producersDeclaration, consumersDeclaration)
}

func TestManyProducersToManyConsumers(t *testing.T) {

	// Arrange
	const queueNameA = "TestManyProducersToManyConsumers_A"
	const queueNameB = "TestManyProducersToManyConsumers_B"
	producersDeclaration := []ProducerSettings{
		{QueueName: queueNameA, Messages: []string{"Yaguareté", "Puma", "Huemul", "Aguará guazú", "Carpincho"}},
		{QueueName: queueNameA, Messages: []string{"Ñandú", "Cóndor andino", "Tatú carreta", "Oso hormiguero gigante", "Gato montés"}},
		{QueueName: queueNameB, Messages: []string{"Zorro gris pampeano", "Mara", "Vizcacha", "Ballena franca austral", "Lobo marino sudamericano"}},
		{QueueName: queueNameB, Messages: []string{"Pingüino de Magallanes", "Guanaco", "Vicuña", "Flamenco austral", "Hornero"}},
	}

	consumersDeclaration := []ConsumerSettings{
		{QueueName: queueNameA},
		{QueueName: queueNameA},
		{QueueName: queueNameB},
		{QueueName: queueNameB},
	}

	DoTestProducerConsumer(t, producersDeclaration, consumersDeclaration)
}

func DoTestProducerConsumer(
	t *testing.T,
	producersDeclaration []ProducerSettings,
	consumersDeclaration []ConsumerSettings,
) {
	// Arrange
	producersByQueue := make(map[string]m.Producer)
	numConsumersByQueue := make(map[string]int)
	conn, err := f.GetConnection(GetConnectionDetails())
	assert.NoError(t, err)
	defer conn.Close()

	for _, producerSettings := range producersDeclaration {
		numConsumersByQueue[producerSettings.QueueName] = 0
	}

	msgsFanIn := make(chan m.Message)
	consumers := make([]m.Consumer, 0, len(consumersDeclaration))
	for _, consumerSettings := range consumersDeclaration {
		middleware, err := conn.Consumer(consumerSettings.QueueName)
		assert.NoError(t, err)

		consumers = append(consumers, middleware)
		numConsumersByQueue[consumerSettings.QueueName] += 1

		// NOTE: Possible error is swallowed
		go middleware.Consume(t.Context(), msgsFanIn)
	}

	// Act
	expectedDeliveries := make([]string, 0)
	for _, producerSettings := range producersDeclaration {
		middleware, err := conn.Producer(producerSettings.QueueName)
		assert.NoError(t, err)
		for _, msg := range producerSettings.Messages {
			expectedDeliveries = append(expectedDeliveries, msg)
			err = middleware.Send(t.Context(), m.Message{Body: []byte(msg)})
			assert.NoError(t, err)
		}
	}

	comparisonResults := make([]bool, 0)
	for range len(expectedDeliveries) {
		msg, ok := <-msgsFanIn
		if !ok {
			break
		}
		msg.Ack()

		Remove(expectedDeliveries, string(msg.Body)) // Does nothing if not present
	}

	close(msgsFanIn)

	// Assert
	assert.Empty(t, comparisonResults)

	for _, producer := range producersByQueue {
		err = producer.Close()
		assert.NoError(t, err)
	}
	for _, consumer := range consumers {
		consumer.StopConsuming()
		err = consumer.Close()
		assert.NoError(t, err)
	}
}
