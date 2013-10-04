import functools
import logging

import gevent
import gevent.queue as Queue

from trpycore.greenlet.util import join

from tramqp_gevent.connection import RabbitConnection
from tramqp_gevent.exceptions import QueueEmpty, QueueFull, QueueStopped
from tramqp_gevent.message import Message

class MessageContextManager(object):
    """Message context manager.

    This class provides a context manager which received messages
    will be wrapped with prior to returning them to the user.
    The context manager ensures that messages are properly
    ack'd following their processing or nack'd if there processing
    results in an uncaught exception.
    """
    def __init__(self,
            ack_queue,
            amqp_msg,
            message_class=Message):
        """MessageContextManager constructor

        Args:
            ack_queue: Queue like object which takes (ack, amqp_msg)
                tuples, where ack is a boolean indicating that an
                ack (True) or nack (False) should be sent for the amqp_msg. 
            amqp_msg: AMQP message object
            message_class: Message like class to use to transform 
                amqp_msg.body into a proper Message like object.
                Note that the message_class must have a static
                parse() method capable of taking amqp_msg.body as
                the sole parameter and returning a message_class
                object.
        """
        self.ack_queue = ack_queue
        self.amqp_msg = amqp_msg
        self.message_class = message_class
    
    def __enter__(self):
        """Context manager enter method."""
        message = self.message_class.parse(str(self.amqp_msg.body))
        return message
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit method.

        Args:
            exc_type: exception type if an exception was raised
                within the scope of the context manager, None otherwise.
            exc_value: exception value if an exception was raised
                within the scope of the context manager, None otherwise.
            traceback: exception traceback if an exception was raised
                within the scope of the context manager, None otherwise.
        """
        if exc_type is not None:
            self.nack()
        else:
            self.ack()
    
    def ack(self):
        #queue up amqp ack to be sent for amqp_msg
        self.ack_queue.put((True, self.amqp_msg))

    def nack(self):
        #queue up amqp nack to be sent for amqp_msg
        self.ack_queue.put((False, self.amqp_msg))
    

class ConsumeQueue(object):
    """Queue for async AMQP message consumption.

    ConsumeQueue allows users to get() messages from an
    AMQP queue. Note that get() will return a MessageContextManager
    wrapping a message_class object. The context manager
    will ensure that an AMQP ack/nack is sent when the message
    has been processed.

    Example:
        with consume_queue.get() as message:
            #process message
    """
    #Convenience exception classes
    Empty = QueueEmpty
    Full = QueueFull
    Stopped = QueueStopped

    #Stop item to put to queue to stop processing
    STOP_ITEM = object()

    def __init__(self,
            connection_config,
            exchange_config,
            queue_config,
            routing_keys=None,
            prefetch_count=1,
            requeue_nack=False,
            queue=None,
            retry_queue=None,
            queue_class=Queue.Queue,
            message_class=Message,
            debug=False):
        """ConsumeQueue constructor.

        Args:
            connection_config: AMQP ConnectionConfig object
            exchange_config: AMQP ExchangeConfig identifying
                exchange to consume messages from.
            queue_config:AMQP QueueConfig identifying queue
                to consume messages from.
            routing_keys: iterable of routing_keys to consume.
            prefetch_count: Maximum number of AMQP messages to
                queue locally for processing. This should be
                approximately set to the number of threads dedicating
                to processing messages. Queueing a message locally
                will prevent other processes from processing the
                message, so it's best not to prefetch too many
                messages.
            queue: Queue like object to use to locally store
                AMQP messages waiting for processing.
            retry_queue: Optional Queue like object which will
                receive message_class objects whose processing
                resulted in an uncaught exception if requeue_nack
                is set to False. This can be useful for republishing
                failed messgages to a retry queue with a x-message-ttl
                and a x-dead-letter-exchange where they will wait to
                ttl and then be published to the dead letter exchange
                where they will re-enter this queue to be tried again.
            queue_class: Queue like class to use to create queue if
                it was not provided.
            message_class: Message like class to envelop the AMQP
                message.body.
            debug: debug flag
        """
        
        self.connection_config = connection_config
        self.exchange_config = exchange_config
        self.queue_config = queue_config
        self.routing_keys = routing_keys or ['']
        self.prefetch_count = prefetch_count
        self.requeue_nack = requeue_nack
        self.queue = queue or queue_class()
        self.retry_queue = retry_queue
        self.message_class = message_class
        self.debug = debug
        self.log = logging.getLogger(self.__class__.__name__)

        #amqp connection
        self.connection = None
        #amqp channel
        self.channel = None
        #queue containing (ack, amqp_msg) tuples where ack is boolean
        #indicating that message has been processed and needs
        #to be ack'd (True) or nack'd (False)
        self.ack_queue = queue_class()
        #running flag indicated queue has been started and not stopped
        self.running = False
        #flag indicated if connected to amqp server
        self.connected = False
        #flag indicated if amqp channel/exchange/queue is ready for consumption
        self.ready = False

        #amqp connection greenlet which must only read from socket
        self.connection_greenlet = None
        #amqp queue greenlet which must only write to socket
        self.queue_greenlet = None
    
    def start(self):
        """Start consume queue"""

        #make sure any existing greenlets have exited before
        #we start up some new ones.
        if not self.running:
            if self.connection_greenlet:
                self.connection_greenlet.join()
            if self.queue_greenlet:
                self.queue_greenlet.join()

            self._remove_stop_items()
            self.running = True
            self.connection_greenlet = gevent.spawn(self.run_connection)
            self.queue_greenlet = gevent.spawn(self.run_queue)

    def run_connection(self):
        """AMQP connection read loop

        Note that this greenlet should only read from self.connection 
        for safety. The only exception is to setup the
        channel/exchange when self.ready is still False. This
        will be okay since the other greenlet will not write
        to the connection self.ready is set to True.
        """
        while self.running:
            try:
                if self.connection is None:
                    self.connection = self._create_connection(
                            self.connection_config)
                else:
                    self.connection.read_frames()
            except Exception as e:
                self.log.exception(e)
                gevent.sleep(1)

        if self.connection:
            self.connection.close()

        self.running = False


    def run_queue(self):
        """ack_queue processing loop

        Note that this greenlet should only write to self.connection 
        for safety.
        """
        while self.running:
            try:
                if not self.ready:
                    gevent.sleep(1)
                else:
                    #use heartbeat as timeout value for block queue.get()
                    #this is arbitrary, but should be non-zero to ensure
                    #that we break out of loop occassionaly to check
                    #if we should keep running.
                    timeout = self.connection_config.heartbeat
                    ack, amqp_msg = self.ack_queue.get(True, timeout)

                    if ack:
                        #send AMQP ack for message
                        self._ack(self.channel, amqp_msg)
                    else:
                        #send AMQP nack for message
                        self._nack(self.channel, amqp_msg, self.requeue_nack)
            except Queue.Empty:
                pass
            except Exception as e:
                self.log.exception(e)
                gevent.sleep(1)

        #Closing the connection here is not needed if
        #connection_config.heartbeat is non-zero (which it should be)
        #but adding it allows the run_connection greenlet to exit
        #more quickly since connection.read_frames() will return
        #as soon connection.close() is called. Otherwise the
        #run_connection greenlet would have to wait up to heartbeat
        #seconds for a new heartbeat message to wake it up and
        #allow it to detect exit conditions.
        if self.connection:
            self.connection.close()

        self.running = False

    
    def get(self, block=True, timeout=None):
        """get AMQP messages for processing.

        Args:
            block: boolean indicating that method should
                block waiting for new messages.
            timeout: if block is True, timeout may be
                set to the maximum number of seconds to
                wait for a new message before raising
                QueueEmpty.
        Returns:
            MessageContextManager object wrapping a
            self.message_class object. The context manager
            ensures that an AMQP ack is sent for the
            message following successful processing or
            a nack is sent if an unhandled exception is
            raised.
        Raises:
            QueueEmpty, QueueStopped
        Example:
            with self.consume_queue.get() as message:
                #process message
        """
        if not self.running:
            raise self.Stopped()

        try:
            amqp_msg = self.queue.get(block, timeout)
            if amqp_msg is self.STOP_ITEM:
                raise self.Stopped()

            message_context =  MessageContextManager(
                    ack_queue=self.ack_queue,
                    amqp_msg=amqp_msg,
                    message_class=self.message_class)
            return message_context
        except Queue.Empty:
            raise self.Empty()

    def stop(self):
        """Stop consume queue."""
        if self.running:
            self.running = False
            #add stop items to awaken greenlet blocked
            #on get() with a QueueStopped Exception.
            self._add_stop_items()

    def join(self, timeout=None):
        """Join consume queue to wait for it to stop."""
        if self.connection_greenlet and self.queue_greenlet:
            join([self.connection_greenlet, self.queue_greenlet], timeout)

    def _create_connection(self, connection_config):
        """Create AMQP connection object

        Args:
            connection_config: ConnectionConfig object
        Returns:
            AMQP Connection object
        """
        if connection_config.transport == 'socket':
            connection_config.transport = 'gevent'

        connection = RabbitConnection(
                host=connection_config.host,
                port=connection_config.port,
                transport=connection_config.transport,
                vhost=connection_config.vhost,
                heartbeat=connection_config.heartbeat,
                open_cb=self._on_connect,
                close_cb=self._on_disconnect,
                logger=self.log,
                debug=self.debug)
        return connection

    def _create_channel(self, connection, exchange_config):
        """Create AMQP Channel object

        Args:
            connection: AMQP connection object
            exchange_config: AMQP ExchangeConfig object
        Returns:
            AMQP Channel object
        """
        channel = connection.channel()
        channel.basic.qos(prefetch_count=self.prefetch_count)
        self._declare_exchange(channel, exchange_config)
        return channel

    def _declare_exchange(self, channel, exchange_config):
        """Declare AMQP Exchange

        Args:
            channel: AMQP channel object
            exchange_config: ExchangeConfig object
        """
        callback = functools.partial(self._on_exchange_declared, channel)

        channel.exchange.declare(
                exchange=exchange_config.exchange,
                type=exchange_config.type,
                passive=exchange_config.passive,
                durable=exchange_config.durable,
                auto_delete=exchange_config.auto_delete,
                internal=exchange_config.internal,
                arguments=exchange_config.arguments,
                cb=callback)

    def _declare_queue(self, channel, queue_config):
        """Declare AMQP Queue

        Args:
            channel: AMQP channel object
            queue_config: QueueConfig object
        """
        callback = functools.partial(self._on_queue_declared, channel)
        channel.queue.declare(
                queue=queue_config.queue,
                passive=queue_config.passive,
                durable=queue_config.durable,
                exclusive=queue_config.exclusive,
                auto_delete=queue_config.auto_delete,
                arguments=queue_config.arguments,
                cb=callback)

    def _bind_queue(self, channel, exchange_config, queue_config):
        """Bind AMQP Queue

        Args:
            channel: AMQP channel object
            exchange_config: ExchangeConfig object
            queue_config: QueueConfig object
        """
        channel.basic.consume(queue=queue_config.queue, consumer=self._on_msg, no_ack=False)
        
        #bind queue for all routing keys
        for routing_key in self.routing_keys:
            callback = functools.partial(self._on_queue_bound, channel,
                    queue_config.queue, routing_key)

            channel.queue.bind(
                    queue=queue_config.queue,
                    exchange=exchange_config.exchange,
                    routing_key=routing_key,
                    cb=callback)

    def _ack(self, channel, amqp_msg):
        """Send AMQP ack for message

        Args:
            channel: AMQP channel object
            amqp_msg: AMQP message object
        """
        tag = amqp_msg.delivery_info["delivery_tag"]
        channel.basic.ack(delivery_tag=tag)

    def _nack(self, channel, amqp_msg, requeue=False):
        """Send AMQP nack for message

        Args:
            channel: AMQP channel object
            amqp_msg: AMQP message object
            requeue: boolean indicating if amqp_msg
                should be requeued.
        """
        tag = amqp_msg.delivery_info["delivery_tag"]
        channel.basic.nack(delivery_tag=tag, requeue=requeue)
        if not requeue:
            self._retry(channel, amqp_msg)
    
    def _retry(self, channel, amqp_msg):
        """Retry AMQP message
        
        This method will be called for failed messages
        when self.requeue_nack is set to False.

        Args:
            channel: AMQP channel object
            amqp_msg: AMQP message object    
        """
        if self.retry_queue:
            message = self.message_class.parse(str(amqp_msg.body))
            self.retry_queue.put(message)

    def _on_connect(self, connection):
        """AMQP connect handler.

        Args:
            connection: AMQP connection object
        """
        self.connected = True
        self.channel = self._create_channel(connection, self.exchange_config)

    def _on_disconnect(self, connection):
        """AMQP disconnect handler

        Args:
            connection: AMQP connection object
        """

        #if queue name starts with 'amq.' it was automatically
        #generated by the AMQP server because it was initially
        #set to ''. It's invalid to request a queue starting
        #with 'amq.' so it's not possible to reuse the same
        #queue name in event of a disconnecton so we set it
        #back to '' and the server generate a new queue name.
        if self.queue_config.queue.startswith('amq.'):
            self.queue_config.queue = ''

        self.ready = False
        self.connected = False
        self.connection = None
        self.channel = None

    def _on_exchange_declared(self, channel):
        """AMQP Exchange declared handler.

        Args:
            channel: AMQP channel object
        """
        self._declare_queue(channel, self.queue_config)

    def _on_queue_declared(self, channel, queue, msg_count, consume_count):
        """AMQP Queue declared handler.

        Args:
            channel: AMQP channel object
            queue: AMQP queue name
            msg_count: AMQP message count
            consume_count: AMQP consume count
        """
        self.queue_config.queue = queue
        self._bind_queue(channel, self.exchange_config, self.queue_config)
    
    def _on_queue_bound(self, channel, queue, routing_key):
        """AMQP Queue bound handler.

        Args:
            channel: AMQP channel object
            queue: AMQP queue name
            routing_key: AMQP routing key
        """
        self.ready = True

    def _on_msg(self, amqp_msg):
        """AMQP msg handler.

        Args:
            amqp_msg: AMQP message object
        """
        self.queue.put(amqp_msg)

    def _add_stop_items(self):
        """Helper method to add STOP_ITEM's to queue."""
        for x in range(100):
            self.queue.put(self.STOP_ITEM)

    def _remove_stop_items(self):
        """Helper method to remove STOP_ITEM's from queue."""
        non_stop_items = []
        try:
            while True:
                item = self.queue.get(block=False)
                if item is not self.STOP_ITEM:
                    non_stop_items.append(item)
        except Queue.Empty:
            for item in non_stop_items:
                self.queue.put(item)
