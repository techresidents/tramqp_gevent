import functools
import logging
import time
import Queue

import gevent
import gevent.queue as Queue
from gevent.event import AsyncResult

from trpycore.greenlet.util import join

from haigha.message import Message as HaighaMessage

from tramqp_gevent.connection import RabbitConnection
from tramqp_gevent.exceptions import QueueEmpty, QueueFull, QueueStopped
from tramqp_gevent.message import Message

class PublishItem(object):
    """Publish Item."""
    def __init__(self, message, priority, routing_key):
        """PublishItem constructor

        Args:
            message: user message object to be sent in amqp
                message body.
            priority: message priority number (lower = higher priority)
            routing_key: amqp message routing key
        """
        self.message = message
        self.priority = priority
        self.routing_key = routing_key
        self.result = AsyncResult()

class PublishQueue(object):
    """Queue for async AMQP message publishing.

    PublishQueue allows users to put messages to a local
    queue which will be asynchronously processed by a
    background greenlet which will publish the message
    to the specified AMQP exchange.

    Note that all publishing is done using the RabbitMQ
    confirms extension in order to guarantee delivery.
    """
    #Convenience exception classes
    Empty = QueueEmpty
    Full = QueueFull
    Stopped = QueueStopped
    
    #Stop item to put to queue to stop processing
    STOP_ITEM = object()

    def __init__(self,
            sender,
            connection_config,
            exchange_config,
            routing_key='',
            stop_item_priority=float("inf"),
            queue=None,
            queue_class=Queue.PriorityQueue,
            message_class=Message,
            debug=False):
        """PublishQueue constructor.

        Args:
            sender: arbitrary string to use in Message.header
                to identity the sender to the recepient.
            connection_config: AMQP ConnectionConfig object
            exchange_config: AMQP ExchangeConfig identifying
                exchange to publish messages to.
            routing_key: default routing_key to publish messages
                with if routing_key is not specified in Message
                or put().
            stop_item_priority: stop item priority (lower = high priority)
                to control whether items in the local queue should
                be published before stopping. By default, stop_item_priority
                is set to infinity to guarantee that all queued messages
                are published prior to stopping.
            queue: Queue like object to use to locally store
                and prioritize (priority, PublishItem) tuples
                prior to publishing. If not provided, queue_class
                will be used to construct a new queue object.
            queue_class: Queue like class to use to create internal
                queue to store and prioritize put() message prior
                to publishing. It makes sense for queue_class
                to be a PriorityQueue like object since items will
                be added to the queue as (priority, PublishItem)
                tuples.
            message_class: Message like class to envelop the AMQP
                message.body.
            debug: debug flag
        """

        self.sender = sender
        self.connection_config = connection_config
        self.exchange_config = exchange_config
        self.routing_key = routing_key
        self.stop_item_priority = stop_item_priority
        self.queue = queue or queue_class()
        self.message_class = message_class
        self.debug = debug
        self.log = logging.getLogger(self.__class__.__name__)
        
        #amqp connection
        self.connection = None
        #amqp channel
        self.channel = None
        #amqp confirms extension sequence number
        self.seq = 0
        #map from amqp confirms sequence no. to (priority, PublishItem) tuple
        self.unack = {}
        #running flag indicated queue has been started and not stopped
        self.running = False
        #flag indicated if connected to amqp server
        self.connected = False
        #flag indicated if amqp channel/exchange is ready for publishing
        self.ready = False
        
        #amqp connection greenlet which must only read from socket
        self.connection_greenlet = None
        #amqp queue greenlet which must only write to socket
        self.queue_greenlet = None
    
    def start(self):
        """Start publish queue."""
        if not self.running:

            #make sure any existing greenlets have exited before
            #we start up some new ones.
            if self.connection_greenlet:
                self.connection_greenlet.join()
            if self.queue_greenlet:
                self.queue_greenlet.join()

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

        while self._keep_running():
            try:
                if self.connection is None:
                    self.connection = self._create_connection(self.connection_config)
                else:
                    self.connection.read_frames()
            except Exception as e:
                self.log.exception(e)
                gevent.sleep(1)

        if self.connection:
            self.connection.close()

        self.running = False

    def run_queue(self):
        """queue processing loop

        Note that this greenlet should only write to self.connection 
        for safety.
        """

        while self._keep_running():
            try:
                if not self.ready:
                    gevent.sleep(1)
                else:
                    #use heartbeat as timeout value for block queue.get()
                    #this is arbitrary, but should be non-zero to ensure
                    #that we break out of loop occassionaly to check
                    #if we should keep running.
                    timeout = self.connection_config.heartbeat
                    priority, item = self.queue.get(True, timeout)

                    if item is self.STOP_ITEM:
                        #drain the queue if we get STOP_ITEM, since
                        #_keep_running() may check for an empty
                        #queue as part of its exit condition.
                        self._drain()
                    else:
                        #increment AMQP confirms extension sequence
                        #number for the next message
                        self.seq += 1

                        #store (priority, PubishItem) in unack dict
                        #with the sequence number as the key. 
                        #AMQP server will ack/nack our publish with
                        #this sequence number as a reference.
                        self.unack[self.seq] = (priority, item)

                        #publish message to AMQP server
                        self._publish(item.message, item.routing_key)
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

    def put(self, msg, block=True, timeout=None, routing_key=None):
        """Put a message to the local queue to be published asynchronously.

        Note that the block and timeout arguments refer to blocking
        and timing out while adding the msg to the local queue.
        Setting block to True, will only block until the message
        has been added to the local queue, not until it has been
        published. To block until it has been published (and ack'd)
        you must publish_queue.put(msg).get().

        Args:
            msg: Message like object or any arbitrary object which
                can be used to construct a Message like object
                with self.message_class(msg). Additionally, msg
                may be a (priority, msg) tuple where priority
                is a number (lower = higher priority) indicating
                the message's priority for publishing. Messages
                with a higher priority will jump ahead of messages
                with lower priority in the local queue and be
                published first.
            block: boolean indicating that the method should
                block until the msg has been added to the
                local queue where it will wait to be published.
            timeout: timeout in seconds to wait to add msg to
                to the local queue before raising QueueFull
                exception.
            routing_key: optional AMQP routing_key to use
                when publishing the message. If not specified,
                self.routing_key will be used.
        Returns:
            AysncResult object which can be used to block until
            the message has been published and ack'd via the
            RabbitMQ confirms extension.
        Raises:
            QueueFull
        """
        if not self.running:
            raise self.Stopped()
        
        #create PublishItem object to add to local queue.
        item = self._build_item(msg, routing_key)

        try:
            self.queue.put((item.priority, item), block, timeout)
            return item.result
        except Queue.Full:
            raise self.Full()

    def stop(self):
        """Stop publish queue."""
        self.running = False

        #add STOP_ITEM which will wake run_queue greenlet
        #to start shutdown process. Depending on self.stop_item_priority
        #STOP_ITEM may be queued behind messages waiting to be
        #published.
        self.queue.put((self.stop_item_priority, self.STOP_ITEM))

    def join(self, timeout=None):
        """Join publish queue to wait for it to stop."""
        if self.connection_greenlet and self.queue_greenlet:
            join([self.connection_greenlet, self.queue_greenlet], timeout)

    def _create_connection(self, config):
        """Create AMQP connection object

        Args:
            config: ConnectionConfig object
        Returns:
            AMQP Connection object
        """
        if config.transport == 'socket':
            config.transport = 'gevent'

        connection = RabbitConnection(
                host=config.host,
                port=config.port,
                transport=config.transport,
                vhost=config.vhost,
                heartbeat=config.heartbeat,
                open_cb=self._on_connect,
                close_cb=self._on_disconnect,
                logger=self.log,
                debug=self.debug)
        return connection

    def _create_channel(self, connection, config):
        """Create AMQP Channel object

        Args:
            connection: AMQP connection object
            config: AMQP ExchangeConfig object
        Returns:
            AMQP Channel object
        """
        channel = connection.channel()
        self._declare_exchange(channel, config)

        #enable RabbitMQ confirms extension which guarantees
        #that all published messages will be ack'd or nack'd
        #by AMQP server.
        channel.confirm.select()
        channel.basic.set_ack_listener(self._on_publish_ack)
        channel.basic.set_nack_listener(self._on_publish_nack)
        return channel

    def _declare_exchange(self, channel, config):
        """Declare AMQP Exchange

        Args:
            channel: AMQP channel object
            config: ExchangeConfig object
        """
        callback = functools.partial(self._on_exchange_declared, channel)

        channel.exchange.declare(
                exchange=config.exchange,
                type=config.type,
                passive=config.passive,
                durable=config.durable,
                auto_delete=config.auto_delete,
                internal=config.internal,
                arguments=config.arguments,
                cb=callback)

    def _build_item(self, msg, routing_key=None):
        """Build PublishItem from put() msg

        Args:
            msg: message passed to put() method.
                Note that this may be a (priority, msg)
                tuple.
            routing_key: optional routing_key to use to
                publish message. If not provided self.routing_key
                will be used.
        Returns:
            PublishItem object.
        """
        #use timestamp as priority if not provided, since this
        #will help to preserve publishing order in the case
        #of connection loss.
        now = time.time()
        priority = now

        if isinstance(msg, tuple):
            priority, msg = msg
        if not isinstance(msg, self.message_class):
            msg = self.message_class(msg)
        if msg.header.sender is None:
            msg.header.sender = self.sender
        if msg.header.routing_key is None:
            if routing_key is not None:
                msg.header.routing_key = routing_key
            else:
                msg.header.routing_key = self.routing_key
        if msg.header.timestamp is None:
            msg.header.timestamp = now

        return PublishItem(msg, priority, msg.header.routing_key)

    def _publish(self, message, routing_key):
        """Publish message to AMQP exchange using routing_key.

        Args:
            message: Message-like object whose str() representation
                will be used as the AMQP message body.
            routing_key: routing_key to publish message with.
        """
        self.channel.basic.publish(
                msg=HaighaMessage(str(message)),
                exchange=self.exchange_config.exchange,
                routing_key=routing_key,
                mandatory=False,
                immediate=False)

    def _requeue_unack(self):
        """Requeue published but unacknowledged messages.

        In the case of an AMQP server error or connection loss
        we'll need to requeue all messages which were published
        but not yet ack'd or nack'd by the AMQP server.
        """
        #to guarantee safety make copy of unack dict
        #and loop over that to requeue items. Otherwise there's
        #a chance that a reconnection could occur quickly
        #while we're still loop over unack and the publish
        #greenlet would modify self.unack while we're iterating.
        unack = dict(self.unack)
        
        #requeue unacknowledged items
        for item in unack.values():
            self.queue.put(item)
        
        #empty self.unack
        #this is safer than self.unack.clear()
        self.unack = {}
    
    def _on_connect(self, connection):
        """AMQP connect handler.

        Args:
            connection: AMQP connection object
        """
        self.seq = 0
        self.connected = True
        self._requeue_unack()
        self.channel = self._create_channel(connection, self.exchange_config)

    def _on_disconnect(self, connection):
        """AMQP disconnect handler

        Args:
            connection: AMQP connection object
        """
        self.ready = False
        self.connected = False
        self.connection = None
        self.channel = None
        self._requeue_unack()

    def _on_exchange_declared(self, channel):
        """AMQP Exchange declared handler.

        Args:
            channel: AMQP channel object
        """
        self.ready = True

    def _on_publish_ack(self, seq):
        """AMQP publish ack handler.
        
        Note that this method will be called for each
        and every ack'd message. Under the hood, Haigha
        takes care of this even when the AMQP server
        ack's multiple messages in a single response.

        Args:
            seq: sequence number of ack'd message
        """
        priority, item = self.unack[seq]

        #remove message for unack dict
        del self.unack[seq]

        #set the AsyncResult returned from put() to
        #None which will causes anyone waiting for
        #the result to be awaken.
        item.result.set()

    def _on_publish_nack(self, seq):
        """AMQP publish nack handler.
        
        Note that this method will be called for each
        and every nack'd message. Under the hood, Haigha
        takes care of this even when the AMQP server
        ack's multiple messages in a single response.

        Args:
            seq: sequence number of nack'd message
        """

        #requeue on nack
        #Note sure if this is the right thing to do or
        #if we should abandon the message.
        self.queue.put(self.unack[seq])
        priority, item = self.unack[seq]
        del self.unack[seq]
    
    def _drain(self):
        """Drain all messages from local queue."""
        while True:
            try:
                self.queue.get(False)
            except Queue.Empty:
                break

    def _keep_running(self):
        """Check if PublishQueue should keep running.

        Returns:
            True if PublishQueue should keep running, False otherwise.
        """

        #don't exit until stop() has been called, queue is empty,
        #and all published messages have been ack'd.
        return self.running or not self.queue.empty() or self.unack
