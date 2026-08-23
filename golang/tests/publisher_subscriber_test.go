package tests

import (
	f "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/stretchr/testify/assert"
	"testing"
)

type PublisherSettings struct {
	Topic    string
	Messages []string
}

type SubscriberOpts struct {
	Topic string
}

var waitOpts = GetWaitOptions()

func TestCanConnectPubSub(t *testing.T) {
	conn, err := f.GetConnection(GetConnectionDetails())
	assert.NoError(t, err)

	_, err = conn.Publisher("topic")
	assert.NoError(t, err)

	_, err = conn.Subscriber("topic")
	assert.NoError(t, err)

	err = conn.Close()
	assert.NoError(t, err)
}

func TestOnePubOneSub(t *testing.T) {
	// Arrange
	const topic = "TestOnePubOneSub"
	publisherssDeclaration := []PublisherSettings{
		{topic, []string{
			"Lionel Messi",
			"Diego Maradona",
			"Ángel Di María",
			"Julián Álvarez",
			"Enzo Fernández",
			"Alexis Mac Allister",
			"Emiliano Martínez",
			"Lautaro Martínez",
			"Rodrigo De Paul",
			"Cuti Romero",
		}},
	}

	subscribersDeclaration := []SubscriberOpts{
		{Topic: topic},
	}

	DoTestPubSub(t, publisherssDeclaration, subscribersDeclaration)
}
func TestManyPubsOneSub(t *testing.T) {
	// Arrange
	const topic = "TestManyPubsOneSub"

	publishersDeclaration := []PublisherSettings{
		{Topic: topic, Messages: []string{
			"Buenos Aires",
			"Córdoba",
			"Santa Fe",
			"Mendoza",
			"Tucumán",
			"Entre Ríos",
			"Salta",
			"Misiones",
		}},
		{Topic: topic, Messages: []string{
			"Chaco",
			"Corrientes",
			"Santiago del Estero",
			"San Juan",
			"Jujuy",
			"Río Negro",
			"Neuquén",
			"Formosa",
		}},
		{Topic: topic, Messages: []string{
			"Chubut",
			"San Luis",
			"Catamarca",
			"La Rioja",
			"La Pampa",
			"Santa Cruz",
			"Tierra del Fuego",
		}},
	}

	subscribersDeclaration := []SubscriberOpts{
		{Topic: topic},
	}

	DoTestPubSub(t, publishersDeclaration, subscribersDeclaration)
}

// ----------------------------------------------------------------------------
// BROADCAST MESSAGING TESTS
// ----------------------------------------------------------------------------
func TestOnePubManySubs(t *testing.T) {
	// Arrange
	const routeKey = "TestOneToMany"
	producersDeclaration := []PublisherSettings{
		{Topic: routeKey, Messages: []string{
			"Ferrari",
			"Porsche",
			"Lamborghini",
			"Mercedes-Benz",
			"BMW",
			"Audi",
			"Tesla",
			"Toyota",
			"Ford",
			"Chevrolet",
			"Aston Martin",
			"Mclaren",
		}},
	}

	subscribersDeclaration := []SubscriberOpts{
		{routeKey},
		{routeKey},
		{routeKey},
	}

	DoTestPubSub(t, producersDeclaration, subscribersDeclaration)
}

func TestManyPubsManySubs(t *testing.T) {
	// Arrange
	const topicA = "ManyPubsManySubs_A"
	const topicB = "ManyPubsManySubs_B"

	publishersDeclaration := []PublisherSettings{
		{Topic: topicA, Messages: []string{"Audi", "Ferrari", "Mclaren"}},
		{Topic: topicA, Messages: []string{"Volkswagen", "Mercedes Benz"}},
		{Topic: topicB, Messages: []string{"Boeing", "Cesna"}},
		{Topic: topicB, Messages: []string{"Embraer", "Embraer", "Piper"}},
	}

	subscribersDeclaration := []SubscriberOpts{
		{topicA},
		{topicA},
		{topicB},
		{topicB},
	}

	DoTestPubSub(t, publishersDeclaration, subscribersDeclaration)
}

func DoTestPubSub(
	t *testing.T,
	publishersDeclaration []PublisherSettings,
	subscriberDeclaration []SubscriberOpts,
) {
	/*
	   El primer argumento de esta función es un arreglo de configuraciones para los publishers,
	   donde se declaran para cada uno el topic y los mensajes a publicar.
	   El segundo es la declaración de los consumidores a qué topics se subscribirá.
	   En base a estos parámetros se configura la topología y se determina si la ejecución fue exitosa o no
	   dependiendo de si todos los mensajes publicados fueron recibidos y procesados por los subscribers.
	   Se contempla también que los mensajes se duplicarán en los casos donde hay más de un subscriber por topic
	   esperando que la cantidad de veces que se procesa el mensaje sea igual a la cantidad de subscribers interesados.
	*/

	// Arrange
	msgsFanIn := make(chan m.Message)
	publishers := make(map[string]m.Publisher)
	numConsumersByTopic := make(map[string]int)

	conn, err := f.GetConnection(GetConnectionDetails())
	assert.NoError(t, err)
	defer conn.Close()

	for _, publisherOpts := range publishersDeclaration {
		numConsumersByTopic[publisherOpts.Topic] = 0
	}

	subscribers := make([]m.Subscriber, 0)
	for _, subscriberOpts := range subscriberDeclaration {
		middleware, err := conn.Subscriber(subscriberOpts.Topic)
		assert.NoError(t, err)
		numConsumersByTopic[subscriberOpts.Topic] += 1

		// NOTE: Possible error is swallowed
		go middleware.Consume(t.Context(), msgsFanIn)
	}

	for topic, numConsumers := range numConsumersByTopic {
		WaitForExchangeBindings(topic, numConsumers, waitOpts)
	}

	// Act

	for _, publisherSettings := range publishersDeclaration {
		WaitForExchangeBindings(
			publisherSettings.Topic,
			numConsumersByTopic[publisherSettings.Topic],
			waitOpts,
		)

		publisher, err := conn.Publisher(publisherSettings.Topic)
		assert.NoError(t, err)

		for _, msg := range publisherSettings.Messages {
			err = publisher.Publish(
				t.Context(),
				m.Message{Body: []byte(msg)},
			)
			assert.NoError(t, err)
		}
	}

	expectedDeliveries := make([]string, 0)
	for _, publisherSettings := range publishersDeclaration {
		numConsumersForKey := 0
		for _, subscriberSettings := range subscriberDeclaration {
			if subscriberSettings.Topic == publisherSettings.Topic {
				numConsumersForKey += 1
			}
		}
		for range numConsumersForKey {
			expectedDeliveries = append(
				expectedDeliveries,
				publisherSettings.Messages...,
			)
		}
	}

	comparisonResults := make([]bool, 0)

	deliveries := len(expectedDeliveries)
	for range deliveries {
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

	for _, publisher := range publishers {
		err = publisher.Close()
		assert.NoError(t, err)
	}

	for _, subscriber := range subscribers {
		subscriber.StopConsuming()
		err = subscriber.Close()
		assert.NoError(t, err)
	}
}
