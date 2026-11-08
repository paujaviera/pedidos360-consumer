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

import cl.duoc.pedidos360_backend.messaging.event.PedidoEvent;;

@Component
public class NotificacionesConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(NotificacionesConsumer.class);

    @RabbitListener(
            queues = "${app.rabbit.queue.notificaciones}",
            ackMode = "MANUAL"
    )
    public void procesar(
            @Payload PedidoEvent evento,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(AmqpHeaders.REDELIVERED) boolean redelivered
    ) throws IOException {

        try {

            if (evento == null) {
                throw new IllegalArgumentException(
                        "El evento recibido es nulo"
                );
            }

            log.info(
                    "Notificación procesada correctamente. Evento: {}",
                    evento
            );

            // Confirmar procesamiento exitoso
            channel.basicAck(deliveryTag, false);

        } catch (IllegalArgumentException e) {

            log.error(
                    "Evento inválido. Enviando a DLQ. Motivo: {}",
                    e.getMessage()
            );

            // Evento inválido: enviar directamente a DLQ
            channel.basicNack(deliveryTag, false, false);

        } catch (Exception e) {

            log.error(
                    "Error procesando notificación. Redelivered: {}",
                    redelivered,
                    e
            );

            // Primer intento fallido: reintentar
            // Segundo intento fallido: enviar a DLQ
            channel.basicNack(
                    deliveryTag,
                    false,
                    !redelivered
            );
        }
    }
}