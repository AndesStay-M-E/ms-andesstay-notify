package cl.duoc.andesstay.notify.messaging.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;

import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología RabbitMQ del consumidor.
 *
 *  andesstay.commands (direct)
 *    └── email.send -> notify.email.q
 *                                 └─(rechazo)─> andesstay.dlx -> notify.email.dlq
 *
 * Los argumentos de la cola deben ser idénticos a los declarados por
 * ms-andesstay-reservations; si no, RabbitMQ rechaza la declaración.
 * Los nombres reales se leen desde application.yaml.
 */
@Configuration
@EnableConfigurationProperties(MessagingProperties.class)
public class RabbitMQConfig {

    private final MessagingProperties properties;

    public RabbitMQConfig(MessagingProperties properties) {
        this.properties = properties;
    }

    /** Exchange direct de comandos (lo publica ms-andesstay-reservations). */
    @Bean
    public DirectExchange commandsExchange() {
        return ExchangeBuilder
                .directExchange(properties.exchanges().commands())
                .durable(true)
                .build();
    }

    /** Dead Letter Exchange: recibe los mensajes rechazados (NACK sin requeue). */
    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder
                .directExchange(properties.exchanges().deadLetter())
                .durable(true)
                .build();
    }

    /** Cola principal: sus rechazos se envían al DLX con la clave de la DLQ. */
    @Bean
    public Queue emailQueue() {
        return QueueBuilder
                .durable(properties.route().queue())
                .deadLetterExchange(properties.exchanges().deadLetter())
                .deadLetterRoutingKey(properties.route().deadLetterQueue())
                .build();
    }

    /** DLQ: guarda los mensajes que no se pudieron procesar. */
    @Bean
    public Queue emailDeadLetterQueue() {
        return QueueBuilder
                .durable(properties.route().deadLetterQueue())
                .build();
    }

    @Bean
    public Binding emailBinding() {
        return BindingBuilder
                .bind(emailQueue())
                .to(commandsExchange())
                .with(properties.route().routingKey());
    }

    @Bean
    public Binding emailDeadLetterBinding() {
        return BindingBuilder
                .bind(emailDeadLetterQueue())
                .to(deadLetterExchange())
                .with(properties.route().deadLetterQueue());
    }

    /**
     * Convierte el JSON al tipo del parámetro del @RabbitListener,
     * ignorando el header __TypeId__ que trae la clase del productor
     * (esa clase no existe en este microservicio).
     */
    @Bean
    public JacksonJsonMessageConverter jsonMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
        converter.setAlwaysConvertToInferredType(true);
        return converter;
    }
}
