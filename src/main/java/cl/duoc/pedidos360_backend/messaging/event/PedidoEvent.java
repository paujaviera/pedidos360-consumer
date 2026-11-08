package cl.duoc.pedidos360_backend.messaging.event;

import java.time.Instant;

public record PedidoEvent(Long id, String producto, String estado, Instant fecha) {}