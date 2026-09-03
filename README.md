# Trabajo Práctico - Middlewares Orientados a Mensajes

## Introducción

Los middlewares orientados a mensajes (MOMs) son un recurso importante para el control de la complejidad en los sistemas distribuídos, puesto que permiten a las distintas partes del sistema comunicarse abstrayéndose de problemas como los cambios de ubicación, fallos, performance y escalabilidad.

En este repositorio se proveen conjuntos de pruebas para las dos formas más comunes de organización de la comunicación sobre colas, que en RabbitMQ se denominan Work Queues y Exchanges.

Se recomienda familiarizarse con estos conceptos leyendo la documentación de RabbitMQ y siguiendo los [tutoriales introductorios](https://www.rabbitmq.com/tutorials).

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas según el lenguaje, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y completar la implementación de las interfaces de middleware provistas, siguiendo los comentarios orientativos sobre su funcionamiento, respetando la abstracción de RabbitMQ y realizando un correcto manejo de errores. No se exige redactar un informe, pero pueden documentarse decisiones de diseño en el archivo `INFORME.md`, si se lo desea.

Se proveen conjuntos de pruebas unitarias para probar la solución. El incumplimiento de las pruebas es condición de desaprobación, pero su cumplimiento no es suficiente para la aprobación. Se pide a los alumnos leer atentamente el enunciado y **tener en cuenta** los criterios de corrección informados [en el campus](https://campusgrado.fi.uba.ar/mod/page/view.php?id=73393).

La entrega consiste en el enlace al último commit que hayan realizado, por ejemplo:
[https://github.com/7574-sistemas-distribuidos/tp-mom/commit/4400022805c9eb3e0cb67c748f9aba74a75ee024](https://github.com/7574-sistemas-distribuidos/tp-mom/commit/4400022805c9eb3e0cb67c748f9aba74a75ee024)

Al momento de la evaluación y ejecución de las pruebas se **descartarán** los cambios realizados a todos los archivos, a excepción de:

**Python:** `/python/src/common/middleware/middleware_rabbitmq.py` 

**Golang:** `/golang/internal/factory/*/*.go` 

## Ejecución

### Comandos del Makefile

`make up` : Inicia contenedores de RabbitMQ  y de pruebas de integración según el lenguaje. Comienza a seguir los logs de las pruebas.

`make down`: Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia contenedores de RabbitMQ y de pruebas de integración, ejecuta las pruebas y libera los recursos asociados.

`make local`: Ejecuta las pruebas de integración desde el host. Ver sección correspondiente.

### Pruebas locales desde el Host

Habiendo iniciado el contenedor de RabbitMQ o configurado una instancia local del mismo puede utilizarse `make local` para ejecutar las pruebas desde el host sin necesidad de detener y reiniciar los contenedores. Para ello se requiere:

#### Python
Instalar una versión de Python superior a `3.14`. Se recomienda emplear un gestor de versiones, como ser `pyenv`.
Instalar los dependencias de la suite de pruebas:
`pip install -r python/src/tests/requirements.txt`

#### Golang
Instalar una versión de Golang superior a `1.24`.
Instalar los dependencias de la suite de pruebas:
`go mod download`
