# ms-andesstay-notify

Microservicio de notificaciones: consume comandos de envío de correo.

| Elemento | Valor |
|---|---|
| Puerto | 8083 |
| Exchange origen | `andesstay.commands` (direct) |
| Routing key | `email.send` |
| Cola | `notify.email.q` |
| DLQ | `notify.email.dlq` (vía `andesstay.dlx`) |

## Manejo de mensajes

- ACK manual (`acknowledge-mode: manual`).
- Error no recuperable (datos inválidos): NACK sin reencolar, el mensaje va a la DLQ y se registra en el log.
- Error recuperable: se reintenta hasta `andesstay.messaging.retry.max-attempts` (3) y luego va a la DLQ.
- JSON inválido: el contenedor lo rechaza sin reencolar (`default-requeue-rejected: false`) y va a la DLQ.

## Ejecutar

1. Levantar RabbitMQ con el repositorio `infra-andesstay-messaging` (`docker compose up -d`).
2. `./mvnw spring-boot:run` (Windows: `mvnw.cmd spring-boot:run`).

## Pruebas

`./mvnw test`
