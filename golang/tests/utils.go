package tests

import (
	"os"
	"slices"
	"strconv"

	r "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware_rabbitmq"
)

func GetCredentials() r.ConnectionConfiguration {
	port, err := strconv.ParseInt(os.Getenv("RABBITMQ_PORT"), 10, 64)
	if err != nil {
		panic("Invalid port number")
	}
	return r.ConnectionConfiguration{Hostname: os.Getenv("RABBITMQ_HOST"), Port: int(port)}
}

// Remove removes the first occurrence of an element from a slice
func Remove[T comparable](slice []T, element T) []T {
	for i, value := range slice {
		if value == element {
			return slices.Delete(slice, i, i+1)
		}
	}
	return slice
}
