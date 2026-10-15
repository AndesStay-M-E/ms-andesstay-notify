package cl.duoc.andesstay.notify.messaging.support;

import cl.duoc.andesstay.notify.messaging.config.MessagingProperties;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Centraliza la confirmación manual de mensajes (ACK / NACK).
 *
 * Política de errores:
 *  - Éxito                  -> basicAck.
 *  - Error NO recuperable   -> basicNack(requeue=false) -> DLX -> DLQ + log ERROR.
 *  - Error recuperable      -> se republica a la misma cola con el header
 *                              x-andesstay-retry-count + 1 y se hace ACK del
 *                              original (así no se bloquea la cola con un
 *                              requeue infinito). Al llegar a max-attempts
 *                              se envía a la DLQ.
 *  - Si no se puede republicar -> basicNack(requeue=true) para no perderlo.
 */
@Component
public class MessageAcknowledger {

    public static final String RETRY_HEADER = "x-andesstay-retry-count";

    /** Exchange por defecto de RabbitMQ: enruta por nombre de cola. */
    private static final String DEFAULT_EXCHANGE = "";

    private static final Logger log =
            LoggerFactory.getLogger(MessageAcknowledger.class);

    private final RabbitTemplate rabbitTemplate;
    private final MessagingProperties properties;

    public MessageAcknowledger(
            RabbitTemplate rabbitTemplate,
            MessagingProperties properties
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
    }

    public void ack(Channel channel, Message message, String reference) {
        long tag = message.getMessageProperties().getDeliveryTag();
        try {
            channel.basicAck(tag, false);
            log.info("ACK mensaje={} cola={}", reference, properties.route().queue());
        } catch (IOException exception) {
            log.error("No se pudo hacer ACK del mensaje={}: {}", reference, exception.getMessage());
        }
    }

    public void deadLetter(
            Channel channel,
            Message message,
            String reference,
            String reason
    ) {
        long tag = message.getMessageProperties().getDeliveryTag();
        try {
            channel.basicNack(tag, false, false);
            log.error(
                    "NACK mensaje={} enviado a DLQ={} motivo={}",
                    reference,
                    properties.route().deadLetterQueue(),
                    reason
            );
        } catch (IOException exception) {
            log.error("No se pudo hacer NACK del mensaje={}: {}", reference, exception.getMessage());
        }
    }

    public void retryOrDeadLetter(
            Channel channel,
            Message message,
            String reference,
            String reason
    ) {
        int attempt = currentAttempt(message) + 1;
        int maxAttempts = properties.retry().maxAttempts();

        if (attempt >= maxAttempts) {
            deadLetter(
                    channel,
                    message,
                    reference,
                    "Se agotaron " + maxAttempts + " intentos. Último error: " + reason
            );
            return;
        }

        long tag = message.getMessageProperties().getDeliveryTag();

        Message retryMessage = MessageBuilder
                .fromMessage(message)
                .setHeader(RETRY_HEADER, attempt)
                .build();

        try {
            rabbitTemplate.send(DEFAULT_EXCHANGE, properties.route().queue(), retryMessage);
            channel.basicAck(tag, false);
            log.warn(
                    "Error recuperable mensaje={} intento={}/{} motivo={}. Se reintenta.",
                    reference,
                    attempt,
                    maxAttempts,
                    reason
            );
        } catch (AmqpException | IOException exception) {
            log.error(
                    "No se pudo reprogramar el mensaje={} ({}). Se devuelve a la cola.",
                    reference,
                    exception.getMessage()
            );
            requeue(channel, tag, reference);
        }
    }

    /** Cuántos reintentos lleva el mensaje (0 si es la primera entrega). */
    public int currentAttempt(Message message) {
        Object value = message.getMessageProperties()
                .getHeaders()
                .get(RETRY_HEADER);

        return value instanceof Number number ? number.intValue() : 0;
    }

    private void requeue(Channel channel, long tag, String reference) {
        try {
            channel.basicNack(tag, false, true);
        } catch (IOException exception) {
            log.error("No se pudo reencolar el mensaje={}: {}", reference, exception.getMessage());
        }
    }
}
