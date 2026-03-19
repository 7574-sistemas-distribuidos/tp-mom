package tests

import (
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

type ExcProdSettings struct {
	MessagesByRoutingKey map[string][]string
}

type ExcConsSettings struct {
	RoutingKeys []string
}

const EXCHANGE_NAME = "test_exchange"

func TestCanConnectExchange(t *testing.T) {
	producer_mw, init_error := GetExchangeMiddleware("test_exchange", []string{"TestCanConnect"})
	assert.NoError(t, init_error)

	close_error := producer_mw.Close()
	assert.NoError(t, close_error)
}

func TestOneToOneExchange(t *testing.T) {
	// Arrange
	prod_settings := []ExcProdSettings{
		{MessagesByRoutingKey: map[string][]string{
			"TestOneToOne": {
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
			},
		}},
	}

	cons_settings := []ExcConsSettings{
		{RoutingKeys: []string{"TestOneToOne"}},
	}

	DoTestExchange(t, prod_settings, cons_settings)
}

func TestOneToManyExchange(t *testing.T) {
	// Arrange
	prod_settings := []ExcProdSettings{
		{MessagesByRoutingKey: map[string][]string{
			"TestOneToMany": {
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
			},
		}},
	}

	cons_settings := []ExcConsSettings{
		{RoutingKeys: []string{"TestOneToOne"}},
		{RoutingKeys: []string{"TestOneToOne"}},
		{RoutingKeys: []string{"TestOneToOne"}},
	}

	DoTestExchange(t, prod_settings, cons_settings)
}

func TestManyToManyExchange(t *testing.T) {
	// Arrange
	prod_settings := []ExcProdSettings{
		{MessagesByRoutingKey: map[string][]string{
			"TestManyToMany_A": {"Audi", "Ferrari", "Mclaren"},
			"TestManyToMany_B": {"Boeing", "Cesna", "Embraer", "Airbus", "Piper"},
		}},
	}

	cons_settings := []ExcConsSettings{
		{RoutingKeys: []string{"TestManyToMany_A"}},
		{RoutingKeys: []string{"TestManyToMany_A", "TestManyToMany_B"}},
		{RoutingKeys: []string{"TestManyToMany_B"}},
	}

	DoTestExchange(t, prod_settings, cons_settings)
}

func TestManyToOneExchange(t *testing.T) {
	// Arrange
	prod_settings := []ExcProdSettings{
		{MessagesByRoutingKey: map[string][]string{
			"TestManyToOne_A": {
				"Buenos Aires",
				"Córdoba",
				"Santa Fe",
				"Mendoza",
				"Tucumán",
				"Entre Ríos",
				"Salta",
				"Misiones",
			},
		}},
		{MessagesByRoutingKey: map[string][]string{
			"TestManyToOne_A": {
				"Chaco",
				"Corrientes",
				"Santiago del Estero",
				"San Juan",
				"Jujuy",
				"Río Negro",
				"Neuquén",
				"Formosa",
			},
		}},
		{MessagesByRoutingKey: map[string][]string{
			"TestManyToOne_A": {
				"Chubut",
				"San Luis",
				"Catamarca",
				"La Rioja",
				"La Pampa",
				"Santa Cruz",
				"Tierra del Fuego",
			},
		}},
	}

	cons_settings := []ExcConsSettings{
		{RoutingKeys: []string{"TestManyToOne_A"}},
	}

	DoTestExchange(t, prod_settings, cons_settings)
}

func DoTestExchange(t *testing.T, producers_settings []ExcProdSettings, consumer_settings []ExcConsSettings) {

	// Arrange
	msgs_fan_in := make(chan string)
	prod_by_key := make(map[string]m.Middleware)
	n_cons_by_key := make(map[string]int)
	for _, p_settings := range producers_settings {
		for routing_key := range p_settings.MessagesByRoutingKey {
			mw, err := GetExchangeMiddleware(EXCHANGE_NAME, []string{routing_key})
			assert.NoError(t, err)
			prod_by_key[routing_key] = mw

			n_cons_by_key[routing_key] = 0
		}
	}

	consumers := make([]m.Middleware, 0)
	for _, c_settings := range consumer_settings {
		mw, err := GetExchangeMiddleware(EXCHANGE_NAME, c_settings.RoutingKeys)
		assert.NoError(t, err)
		consumers = append(consumers, mw)
		for _, key := range c_settings.RoutingKeys {
			n_cons_by_key[key] += 1
		}

		go mw.StartConsuming(func(msg m.Message) { msgs_fan_in <- msg.Body })
	}

	// Act
	for _, p_settings := range producers_settings {
		for routing_key, msgs := range p_settings.MessagesByRoutingKey {
			WaitForExchangeBindings(EXCHANGE_NAME, routing_key, n_cons_by_key[routing_key], w_opts)
			for _, msg := range msgs {
				prod_by_key[routing_key].Send(m.Message{Body: msg})
			}
		}
	}

	expected_deliveries := make([]string, 0)
	for _, p_settings := range producers_settings {
		for key, msgs := range p_settings.MessagesByRoutingKey {
			consumers_for_key := 0
			for _, c_settings := range consumer_settings {
				if slices.Contains(c_settings.RoutingKeys, key) {
					consumers_for_key += 1
				}
			}
			for range consumers_for_key {
				expected_deliveries = append(expected_deliveries, msgs...)
			}
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

	for _, p := range prod_by_key {
		close_err := p.Close()
		assert.NoError(t, close_err)
	}

	for _, c := range consumers {
		c.StopConsuming()
		close_err := c.Close()
		assert.NoError(t, close_err)
	}
}
