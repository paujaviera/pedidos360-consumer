package cl.duoc.pedidos360_consumer.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    @Value("${app.rabbit.exchange.pedidos}") private String pedidosExchange;
    @Value("${app.rabbit.exchange.dlx}") private String dlxExchange;

    @Value("${app.rabbit.queue.notificaciones}") private String notificacionesQueue;
    @Value("${app.rabbit.queue.notificaciones-dlq}") private String notificacionesDlq;
    @Value("${app.rabbit.queue.tickets}") private String ticketsQueue;
    @Value("${app.rabbit.queue.tickets-dlq}") private String ticketsDlq;
    @Value("${app.rabbit.queue.documentos}") private String documentosQueue;
    @Value("${app.rabbit.queue.documentos-dlq}") private String documentosDlq;

    @Value("${app.rabbit.routing-key.pedido-creado}") private String pedidoCreadoKey;
    @Value("${app.rabbit.routing-key.pedido-estado}") private String pedidoEstadoKey;
    @Value("${app.rabbit.routing-key.notificaciones-binding}") private String notificacionesBindingKey;
    @Value("${app.rabbit.routing-key.notificaciones-dlq}") private String notificacionesDlqKey;
    @Value("${app.rabbit.routing-key.tickets-dlq}") private String ticketsDlqKey;
    @Value("${app.rabbit.routing-key.documentos-dlq}") private String documentosDlqKey;

    // ---------- Exchanges ----------
    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(pedidosExchange, true, false);
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(dlxExchange, true, false);
    }

    // ---------- Colas principales ----------
    @Bean
    public Queue notificacionesQueue() {
        return QueueBuilder.durable(notificacionesQueue)
                .deadLetterExchange(dlxExchange)
                .deadLetterRoutingKey(notificacionesDlqKey).build();
    }

    @Bean
    public Queue ticketsQueue() {
        return QueueBuilder.durable(ticketsQueue)
                .deadLetterExchange(dlxExchange)
                .deadLetterRoutingKey(ticketsDlqKey).build();
    }

    @Bean
    public Queue documentosQueue() {
        return QueueBuilder.durable(documentosQueue)
                .deadLetterExchange(dlxExchange)
                .deadLetterRoutingKey(documentosDlqKey).build();
    }

    // ---------- DLQs ----------
    @Bean
    public Queue notificacionesDlq() {
        return QueueBuilder.durable(notificacionesDlq).build();
    }

    @Bean
    public Queue ticketsDlq() {
        return QueueBuilder.durable(ticketsDlq).build();
    }

    @Bean
    public Queue documentosDlq() {
        return QueueBuilder.durable(documentosDlq).build();
    }

    // ---------- Bindings ----------
    @Bean
    public Binding notificacionesBinding() {
        return BindingBuilder.bind(notificacionesQueue()).to(pedidosExchange()).with(notificacionesBindingKey);
    }

    @Bean
    public Binding ticketsBinding() {
        return BindingBuilder.bind(ticketsQueue()).to(pedidosExchange()).with(pedidoCreadoKey);
    }

    @Bean
    public Binding documentosBinding() {
        return BindingBuilder.bind(documentosQueue()).to(pedidosExchange()).with(pedidoEstadoKey);
    }

    @Bean
    public Binding notificacionesDlqBinding() {
        return BindingBuilder.bind(notificacionesDlq()).to(dlxExchange()).with(notificacionesDlqKey);
    }

    @Bean
    public Binding ticketsDlqBinding() {
        return BindingBuilder.bind(ticketsDlq()).to(dlxExchange()).with(ticketsDlqKey);
    }

    @Bean
    public Binding documentosDlqBinding() {
        return BindingBuilder.bind(documentosDlq()).to(dlxExchange()).with(documentosDlqKey);
    }

    // ---------- Serialización JSON ----------
    @Bean
    public MessageConverter messageConverter() {
        return new JacksonJsonMessageConverter("cl.duoc.pedidos360_backend.messaging.event");
    }
}