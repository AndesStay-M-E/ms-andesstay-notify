package cl.duoc.andesstay.notify.messaging.consumer;

import cl.duoc.andesstay.notify.exception.NonRecoverableMessageException;
import cl.duoc.andesstay.notify.exception.RecoverableMessageException;
import cl.duoc.andesstay.notify.messaging.model.MessageEnvelope;
import cl.duoc.andesstay.notify.messaging.model.EmailCommandPayload;
import cl.duoc.andesstay.notify.messaging.support.MessageAcknowledger;
import cl.duoc.andesstay.notify.notification.NotificationService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor de la cola notify.email.q (routing key email.send).
 *
 * Solo traduce el mensaje y decide ACK / reintento / DLQ.
 * La lógica de negocio está en NotificationService.
 */
@Component
public class EmailCommandListener {

    private static final Logger log =
            LoggerFactory.getLogger(EmailCommandListener.class);

    private final NotificationService notificationService;
    private final MessageAcknowledger acknowledger;

    public EmailCommandListener(
            NotificationService notificationService,
            MessageAcknowledger acknowledger
    ) {
        this.notificationService = notificationService;
        this.acknowledger = acknowledger;
    }

    @RabbitListener(
            id = "notify-email-listener",
            queues = "${andesstay.messaging.route.queue}"
    )
    public void onEmailMessage(
            @Payload MessageEnvelope<EmailCommandPayload> envelope,
            Message message,
            Channel channel
    ) {
        String reference = envelope.reference();

        log.info(
                "Mensaje recibido type={} eventId={} correlationId={} intento={}",
                envelope.type(),
                reference,
                envelope.correlationId(),
                acknowledger.currentAttempt(message) + 1
        );

        try {
            notificationService.process(envelope);
            acknowledger.ack(channel, message, reference);

        } catch (NonRecoverableMessageException exception) {
            acknowledger.deadLetter(channel, message, reference, exception.getMessage());

        } catch (RecoverableMessageException exception) {
            acknowledger.retryOrDeadLetter(channel, message, reference, exception.getMessage());

        } catch (RuntimeException exception) {
            // Error no previsto: se trata como temporal y se reintenta.
            acknowledger.retryOrDeadLetter(
                    channel,
                    message,
                    reference,
                    "Error inesperado: " + exception.getMessage()
            );
        }
    }
}
