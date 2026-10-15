package cl.duoc.andesstay.notify.messaging.model;

import cl.duoc.andesstay.notify.exception.NonRecoverableMessageException;

import java.time.Instant;
import java.util.UUID;

/**
 * Sobre común de todos los mensajes AndesStay (mismo JSON que
 * CommandEnvelope en ms-andesstay-reservations).
 */
public record MessageEnvelope<T>(
        String type,
        UUID eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        T payload
) {

    /** Identificador para los logs. */
    public String reference() {
        return eventId != null ? eventId.toString() : "sin-eventId";
    }

    /** Un mensaje sin payload nunca se podrá procesar: va a DLQ. */
    public T requirePayload() {
        if (payload == null) {
            throw new NonRecoverableMessageException(
                    "El mensaje " + reference() + " no trae payload"
            );
        }
        return payload;
    }
}
