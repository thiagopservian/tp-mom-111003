import pika
from pika.exceptions import AMQPConnectionError
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Implementación de MessageMiddlewareQueue sobre RabbitMQ (Work Queue).

    Permite enviar y consumir mensajes de una cola nombrada. Múltiples
    consumidores compiten por los mensajes (cada mensaje es entregado a
    exactamente un consumidor).
    """

    def __init__(self, host, queue_name):
        """
        Inicializa la conexión y declara la cola.

        Parámetros:
        - host: host/IP del broker RabbitMQ.
        - queue_name: nombre de la cola a usar (se crea si no existe).
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def send(self, message):
        """
        Publica un mensaje en la cola configurada.

        Parámetros:
        - message: contenido a publicar (bytes).

        Eleva MessageMiddlewareDisconnectedError si se pierde la conexión.
        """
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def start_consuming(self, on_message_callback):
        """
        Inicia el consumo bloqueante de mensajes.

        Parámetros:
        - on_message_callback: función con firma callback(message, ack, nack).
          - message: cuerpo del mensaje (bytes).
          - ack: función sin argumentos para confirmar el mensaje.
          - nack: función sin argumentos para rechazar el mensaje.

        Eleva MessageMiddlewareDisconnectedError si se pierde la conexión.
        Eleva MessageMiddlewareMessageError ante error interno irrecuperable.
        """
        def callback(ch, method, properties, body):
            try:
                on_message_callback(
                    body,
                    lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                    lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
                )
            except MessageMiddlewareDisconnectedError:
                raise
            except Exception as e:
                raise MessageMiddlewareMessageError(
                    f"An error occurred while processing the message: {str(e)}"
                )

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        """
        Detiene el consumo iniciado con start_consuming().

        Si no se estaba consumiendo, no tiene efecto ni eleva excepciones.
        """
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        """
        Cierra la conexión con RabbitMQ.

        Eleva MessageMiddlewareCloseError si ocurre un error al cerrar.
        """
        try:
            self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Implementación de MessageMiddlewareExchange sobre RabbitMQ (Exchange topic).

    Permite publicar mensajes a un exchange con routing keys específicas y
    suscribirse a mensajes filtrando por una o más routing keys. Múltiples
    consumidores suscritos a la misma routing key reciben todos una copia
    del mensaje (broadcast).
    """

    def __init__(self, host, exchange_name, routing_keys):
        """
        Inicializa la conexión y declara el exchange.

        La cola de consumo se crea de forma diferida en start_consuming(),
        lo que permite que los bindings estén activos antes de que el
        productor publique.

        Parámetros:
        - host: host/IP del broker RabbitMQ.
        - exchange_name: nombre del exchange (se crea si no existe).
        - routing_keys: lista de routing keys para publicar/suscribirse.
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    def send(self, message):
        """
        Publica el mensaje en el exchange para cada routing key configurada.

        Si no hay routing keys, publica una vez con routing key vacía.

        Parámetros:
        - message: contenido a publicar (bytes).

        Eleva MessageMiddlewareDisconnectedError si se pierde la conexión.
        """
        try:
            routing_keys = self.routing_keys if self.routing_keys else ['']
            for routing_key in routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ")

    def start_consuming(self, on_message_callback):
        """
        Crea una cola anónima exclusiva, la vincula a las routing keys y
        comienza el consumo bloqueante.

        La cola es exclusiva (un solo consumidor) y se elimina automáticamente
        al cerrar la conexión. Cada instancia consumidora recibe su propia
        copia de los mensajes publicados a las routing keys suscritas.

        Parámetros:
        - on_message_callback: función con firma callback(message, ack, nack).

        Eleva MessageMiddlewareDisconnectedError si se pierde la conexión.
        Eleva MessageMiddlewareMessageError ante error interno irrecuperable.
        """
        result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        self.queue_name = result.method.queue

        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

        def callback(ch, method, properties, body):
            try:
                on_message_callback(
                    body,
                    lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                    lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
                )
            except MessageMiddlewareDisconnectedError:
                raise
            except Exception as e:
                raise MessageMiddlewareMessageError(
                    f"An error occurred while processing the message: {str(e)}"
                )

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        """
        Detiene el consumo iniciado con start_consuming().

        Si no se estaba consumiendo, no tiene efecto ni eleva excepciones.
        """
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        """
        Cierra la conexión con RabbitMQ.

        Eleva MessageMiddlewareCloseError si ocurre un error al cerrar.
        """
        try:
            self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError("Failed to close the connection to RabbitMQ")
