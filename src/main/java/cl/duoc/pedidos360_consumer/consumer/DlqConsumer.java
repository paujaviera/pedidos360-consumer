package cl.duoc.pedidos360_consumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    @RabbitListener(queues = {
            "${app.rabbit.queue.notificaciones-dlq}",
            "${app.rabbit.queue.tickets-dlq}",
            "${app.rabbit.queue.documentos-dlq}"
    })
    public void registrar(Message mensaje, Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {

        log.error("[DLQ] Mensaje no entregado desde cola '{}': {}",
                mensaje.getMessageProperties().getConsumerQueue(),
                new String(mensaje.getBody()));

        channel.basicAck(tag, false);
    }
}