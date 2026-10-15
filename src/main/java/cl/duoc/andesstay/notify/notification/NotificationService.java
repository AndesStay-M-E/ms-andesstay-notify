package cl.duoc.andesstay.notify.notification;

import cl.duoc.andesstay.notify.exception.NonRecoverableMessageException;
import cl.duoc.andesstay.notify.exception.RecoverableMessageException;
import cl.duoc.andesstay.notify.messaging.model.EmailCommandPayload;
import cl.duoc.andesstay.notify.messaging.model.MessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Lógica de negocio de notificaciones (no conoce RabbitMQ).
 *
 * El envío real de correo queda simulado con un log.
 */
@Service
public class NotificationService {

    private static final Logger log =
            LoggerFactory.getLogger(NotificationService.class);

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    /**
     * Para la demostración: un destinatario con este dominio simula
     * que el servidor de correo está caído (error recuperable).
     */
    public static final String SIMULATED_OUTAGE_DOMAIN = "@falla-temporal.test";

    private static final Map<String, String> SUBJECTS = Map.of(
            "RESERVATION_CONFIRMED", "Tu reserva fue confirmada",
            "CHECKIN_REMINDER", "Recordatorio de check-in",
            "CHECKOUT_COMPLETED", "Gracias por hospedarte en AndesStay"
    );

    public void process(MessageEnvelope<EmailCommandPayload> envelope) {
        EmailCommandPayload payload = envelope.requirePayload();

        if (payload.reservationId() == null) {
            throw new NonRecoverableMessageException("El correo no indica la reserva");
        }

        String recipient = payload.recipient();

        if (recipient == null || !EMAIL_PATTERN.matcher(recipient).matches()) {
            throw new NonRecoverableMessageException(
                    "Destinatario de correo inválido: " + recipient
            );
        }

        String subject = SUBJECTS.get(payload.notificationType());

        if (subject == null) {
            throw new NonRecoverableMessageException(
                    "Tipo de notificación desconocido: " + payload.notificationType()
            );
        }

        if (recipient.toLowerCase(Locale.ROOT).endsWith(SIMULATED_OUTAGE_DOMAIN)) {
            throw new RecoverableMessageException(
                    "Servidor de correo no disponible (simulado)"
            );
        }

        log.info(
                "Correo enviado a={} asunto=\"{}\" reserva={} traceId={}",
                recipient,
                subject,
                payload.reservationId(),
                envelope.traceId()
        );
    }
}
