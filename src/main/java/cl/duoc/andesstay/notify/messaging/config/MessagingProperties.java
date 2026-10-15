package cl.duoc.andesstay.notify.messaging.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombres de exchanges, cola, routing key y DLQ leídos desde
 * application.yaml (prefijo andesstay.messaging).
 *
 * Ningún nombre de RabbitMQ se escribe en el código: todos salen de aquí.
 */
@Validated
@ConfigurationProperties(prefix = "andesstay.messaging")
public record MessagingProperties(

        @Valid @NotNull
        Exchanges exchanges,

        @Valid @NotNull
        Route route,

        @Valid @NotNull
        Retry retry

) {

    public record Exchanges(
            @NotBlank String commands,
            @NotBlank String events,
            @NotBlank String deadLetter
    ) {
    }

    /**
     * queue: cola que consume este microservicio.
     * routingKey: clave con la que la cola se enlaza al exchange.
     * deadLetterQueue: DLQ; también es la routing key dentro del DLX.
     */
    public record Route(
            @NotBlank String queue,
            @NotBlank String routingKey,
            @NotBlank String deadLetterQueue
    ) {
    }

    public record Retry(
            @Min(1) @Max(10) int maxAttempts
    ) {
    }
}
