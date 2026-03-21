# Trabajo Práctico - Middleware Orientado a Mensajes

## Ejecución

`make up` : Inicia contenedores de RabbitMQ  y de pruebas de integración. Comienza a seguir los logs de las pruebas.

`make down`:   Detiene los contenedores de pruebas y destruye los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

## Pruebas desde el host

Habiendo ejecutado `make up` pueden ejecutarse las pruebas sin tener que detener y reiniciar los contenedores ejecutando `make local`, siempre que se cumplan los siguientes requisitos.

### Python
Instalar una versión de Python superior a `3.14`. Se recomienda emplear un gestor de versiones, como ser `pyenv`.
Instalar los dependencias de la suite de pruebas:
`pip install -r src/tests/requirements.txt`

### Golang
Instalar una versión de Golang superior a `1.24`.
Instalar los dependencias de la suite de pruebas:
`go mod download`
