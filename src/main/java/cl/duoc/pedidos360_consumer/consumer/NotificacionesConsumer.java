package cl.duoc.pedidos360_consumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.pedidos360_backend.messaging.event.PedidoEvent;

@Component
public class NotificacionesConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificacionesConsumer.class);

    @RabbitListener(queues = "${app.rabbit.queue.notificaciones}")
    public void procesar(@Payload PedidoEvent evento,
                         Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                         @Header(AmqpHeaders.REDELIVERED) boolean redelivered) throws IOException {
        try {
            log.info("[NOTIFICACION] Pedido {} de '{}' en estado {}",
                    evento.id(), evento.producto(), evento.estado());
            channel.basicAck(tag, false);
        } catch (IllegalArgumentException e) {
            log.error("[NOTIFICACION] Error no recuperable, enviando a DLQ: {}", e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            log.warn("[NOTIFICACION] Error (redelivered={}): {}", redelivered, e.getMessage());
            channel.basicNack(tag, false, !redelivered);
        }
    }
}