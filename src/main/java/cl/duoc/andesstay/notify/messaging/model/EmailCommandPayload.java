package cl.duoc.andesstay.notify.messaging.model;

public record EmailCommandPayload(
        Long reservationId,
        String recipient,
        String notificationType
) {
}
