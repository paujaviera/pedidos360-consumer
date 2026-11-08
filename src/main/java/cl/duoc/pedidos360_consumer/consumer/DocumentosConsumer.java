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
public class DocumentosConsumer {

    private static final Logger log = LoggerFactory.getLogger(DocumentosConsumer.class);

    @RabbitListener(queues = "${app.rabbit.queue.documentos}")
    public void procesar(@Payload PedidoEvent evento,
                         Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                         @Header(AmqpHeaders.REDELIVERED) boolean redelivered) throws IOException {
        try {
            if ("INVALIDO".equalsIgnoreCase(evento.estado())) {
                throw new IllegalArgumentException("Estado inválido para generar documento");
            }
            log.info("[DOCUMENTO] Comprobante DOC-{} generado para '{}' (estado {})",
                    evento.id(), evento.producto(), evento.estado());
            channel.basicAck(tag, false);
        } catch (IllegalArgumentException e) {
            log.error("[DOCUMENTO] Error no recuperable, enviando a DLQ: {}", e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            log.warn("[DOCUMENTO] Error (redelivered={}): {}", redelivered, e.getMessage());
            channel.basicNack(tag, false, !redelivered);
        }
    }
}