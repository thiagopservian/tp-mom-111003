# Trabajo Práctico - Middlewares Orientados a Mensajes

## Implementación (Python)

El único archivo modificado es:

```
python/src/common/middleware/middleware_rabbitmq.py
```

Este archivo implementa las interfaces abstractas definidas en `middleware.py`:

| Clase | Patrón | Descripción |
|---|---|---|
| `MessageMiddlewareQueueRabbitMQ` | Work Queue | Cada mensaje es entregado a **exactamente un** consumidor. Múltiples consumidores compiten. |
| `MessageMiddlewareExchangeRabbitMQ` | Exchange (topic) | Cada consumidor recibe su **propia copia** de los mensajes publicados a las routing keys a las que está suscripto. |

### Diseño de la Queue

- Se conecta a RabbitMQ y declara la cola en `__init__`. La declaración es idempotente: múltiples productores/consumidores pueden declararla sin conflicto.
- `send()` publica al **default exchange** con la cola como routing key.
- `start_consuming()` inicia un consumo bloqueante con `auto_ack=False`. El callback recibe `(message, ack, nack)` y decide cuándo confirmar.
- `stop_consuming()` detiene el loop. Si no se estaba consumiendo, no tiene efecto (protegido con `try/except`).

### Diseño del Exchange

- En `__init__` se declara el exchange como tipo `topic`. **No** se crea la cola aún.
- La cola se crea en `start_consuming()`, justo antes de activar el consumo. Es anónima, exclusiva y con `auto_delete=True`, lo que garantiza que:
  - Cada consumidor tiene su propia cola → **cada uno recibe todos los mensajes** (broadcast).
  - La cola se elimina automáticamente al cerrar la conexión.
- Se hace `queue_bind` para cada routing key antes de empezar a consumir, garantizando que los bindings están activos cuando el productor publica.
- `send()` publica al exchange una vez por cada routing key configurada.
- `stop_consuming()` también está protegido con `try/except`.

### Manejo de errores

| Situación | Excepción elevada |
|---|---|
| Pérdida de conexión al enviar/consumir | `MessageMiddlewareDisconnectedError` |
| Error interno en el procesamiento | `MessageMiddlewareMessageError` |
| Error al cerrar la conexión | `MessageMiddlewareCloseError` |

---

## Resultados de las pruebas

Ejecutadas con `make up` sobre Python 3.14.3 (Alpine, Docker):

```
tests  | ============================= test session starts ==============================
tests  | platform linux -- Python 3.14.3, pytest-9.0.2, pluggy-1.6.0
tests  | rootdir: /
tests  | configfile: pytest.ini
tests  | testpaths: test_queue.py, test_exchange.py
tests  | plugins: timeout-2.4.0
tests  | timeout: 10.0s
tests  | timeout method: signal
tests  | timeout func_only: False
tests  | collected 19 items
tests  | 
tests  | test_queue.py .............                                              [ 68%]
tests  | test_exchange.py ......                                                  [100%]
tests  | 
tests  | ============================== 19 passed in 2.71s ==============================
```

---



## Ejecución

```bash
make up      # Inicia RabbitMQ y corre las pruebas en Docker
make down    # Detiene y destruye los contenedores
make logs    # Sigue los logs de todos los contenedores
```




