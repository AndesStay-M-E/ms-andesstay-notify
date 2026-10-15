package cl.duoc.andesstay.notify.messaging.consumer;

import cl.duoc.andesstay.notify.exception.NonRecoverableMessageException;
import cl.duoc.andesstay.notify.exception.RecoverableMessageException;
import cl.duoc.andesstay.notify.messaging.config.MessagingProperties;
import cl.duoc.andesstay.notify.messaging.model.MessageEnvelope;
import cl.duoc.andesstay.notify.messaging.model.EmailCommandPayload;
import cl.duoc.andesstay.notify.messaging.support.MessageAcknowledger;
import cl.duoc.andesstay.notify.notification.NotificationService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Instant;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Prueba la política de ACK / reintento / DLQ sin necesitar RabbitMQ.
 */
class EmailCommandListenerTest {

    private static final long DELIVERY_TAG = 7L;

    private NotificationService service;
    private RabbitTemplate rabbitTemplate;
    private Channel channel;
    private EmailCommandListener listener;
    private MessageEnvelope<EmailCommandPayload> envelope;

    @BeforeEach
    void setUp() {
        MessagingProperties properties = new MessagingProperties(
                new MessagingProperties.Exchanges("andesstay.commands", "andesstay.events", "andesstay.dlx"),
                new MessagingProperties.Route("notify.email.q", "email.send", "notify.email.dlq"),
                new MessagingProperties.Retry(3)
        );

        service = mock(NotificationService.class);
        rabbitTemplate = mock(RabbitTemplate.class);
        channel = mock(Channel.class);

        listener = new EmailCommandListener(
                service,
                new MessageAcknowledger(rabbitTemplate, properties)
        );

        envelope = new MessageEnvelope<>(
                "email.send",
                UUID.randomUUID(),
                Instant.now(),
                "trace-1",
                "reservation-1",
                new EmailCommandPayload(1L, "cliente@andesstay.cl", "RESERVATION_CONFIRMED")
        );
    }

    @Test
    void procesamientoCorrectoHaceAck() throws Exception {
        listener.onEmailMessage(envelope, message(0), channel);

        verify(channel).basicAck(DELIVERY_TAG, false);
        verify(channel, never()).basicNack(anyLong(), eq(false), eq(false));
    }

    @Test
    void errorNoRecuperableEnviaADlq() throws Exception {
        doThrow(new NonRecoverableMessageException("dato inválido"))
                .when(service).process(any());

        listener.onEmailMessage(envelope, message(0), channel);

        verify(channel).basicNack(DELIVERY_TAG, false, false);
        verify(rabbitTemplate, never()).send(any(String.class), any(String.class), any(Message.class));
    }

    @Test
    void errorRecuperableSeReintenta() throws Exception {
        doThrow(new RecoverableMessageException("servicio caído"))
                .when(service).process(any());

        listener.onEmailMessage(envelope, message(0), channel);

        verify(rabbitTemplate).send(eq(""), eq("notify.email.q"), any(Message.class));
        verify(channel).basicAck(DELIVERY_TAG, false);
    }

    @Test
    void errorRecuperableSinIntentosRestantesVaADlq() throws Exception {
        doThrow(new RecoverableMessageException("servicio caído"))
                .when(service).process(any());

        // Ya lleva 2 reintentos: este es el intento 3 de 3.
        listener.onEmailMessage(envelope, message(2), channel);

        verify(channel).basicNack(DELIVERY_TAG, false, false);
        verify(rabbitTemplate, never()).send(any(String.class), any(String.class), any(Message.class));
    }

    private Message message(int previousRetries) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(DELIVERY_TAG);
        if (previousRetries > 0) {
            properties.setHeader(MessageAcknowledger.RETRY_HEADER, previousRetries);
        }
        return new Message(new byte[0], properties);
    }
}
