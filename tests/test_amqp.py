import unittest

import gevent

import testbase
from tramqp_gevent.config import ConnectionConfig, ExchangeConfig, QueueConfig
from tramqp_gevent.exceptions import QueueEmpty, QueueStopped
from tramqp_gevent.consume import ConsumeQueue
from tramqp_gevent.publish import PublishQueue

class TestQueue(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.connection_config = ConnectionConfig('localdev')
        cls.exchange_config = ExchangeConfig('unittest', 'topic')
        cls.queue_config = QueueConfig()

        cls.publish_queue = PublishQueue(
                sender='unittester',
                connection_config=cls.connection_config,
                exchange_config=cls.exchange_config,
                routing_key='test')

        cls.consume_queue = ConsumeQueue(
                connection_config=cls.connection_config,
                exchange_config=cls.exchange_config,
                queue_config=cls.queue_config,
                routing_keys=['test', 'test.other'])

        cls.publish_queue.start()
        cls.consume_queue.start()

        #give time for connections to be established
        gevent.sleep(1)
    
    @classmethod
    def tearDownClass(cls):
        cls.publish_queue.stop()
        cls.consume_queue.stop()

        cls.publish_queue.join()
        cls.consume_queue.join()

    def test_publish(self):
        results = []
        messages = ['a', 'b', 'c', 'd', 'e']
        for m in messages:
            results.append(self.publish_queue.put(m))
        
        #wait for messages to be ack'd
        for result in results:
            result.get()
        
        for m in messages:
            with self.consume_queue.get() as msg:
                self.assertEqual(m, msg.body)
        
        self.assertTrue(self.publish_queue.queue.empty())
        self.assertFalse(self.publish_queue.unack)

    def test_publish_with_routing_key(self):
        self.publish_queue.put('r', routing_key='test.other')
        with self.consume_queue.get() as m:
            self.assertEqual('r', m.body)
        
    def test_publish_off_topic(self):
        self.publish_queue.put('off_topic', routing_key='off_topic')

        with self.assertRaises(QueueEmpty):
            self.consume_queue.get(False)

    def test_publish_stopped(self):
        queue = PublishQueue(
                sender='unittester',
                connection_config=self.connection_config,
                exchange_config=self.exchange_config)

        with self.assertRaises(QueueStopped):
            queue.put('a')

    def test_disconnect(self):
        self.publish_queue.connection.close()
        self.consume_queue.connection.close()
        gevent.sleep(1)

        self.publish_queue.put('a')
        with self.consume_queue.get() as m:
            self.assertEqual('a', m.body)

    def test_requeue(self):
        #test no requeue
        self.publish_queue.put('a')
        with self.assertRaises(Exception):
            with self.consume_queue.get() as m:
                self.assertEqual('a', m.body)
                raise Exception('nack')
        with self.assertRaises(QueueEmpty):
            with self.consume_queue.get(True, 1) as m:
                self.assertEqual('a', m.body)

        #test requeue
        self.publish_queue.put('a')
        self.consume_queue.requeue_nack = True
        with self.assertRaises(Exception):
            with self.consume_queue.get() as m:
                self.assertEqual('a', m.body)
                raise Exception('nack')
        with self.consume_queue.get(True, 1) as m:
            self.assertEqual('a', m.body)
        self.consume_queue.requeue_nack = False


class TestJobQueue(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.connection_config = ConnectionConfig('localdev')
        cls.exchange_config = ExchangeConfig('unittest_job', 'topic')
        cls.queue_config = QueueConfig()

        cls.retry_exchange_config = ExchangeConfig('unittest_job_retry', 'topic', durable=True)
        arguments = {'x-dead-letter-exchange': 'unittest_job', 'x-message-ttl': 1000 }
        cls.retry_queue_config = QueueConfig('unittest_job_retry_queue', durable=True, auto_delete=False,arguments=arguments)

        cls.create_job_queue = PublishQueue(
                sender='unittester',
                connection_config=cls.connection_config,
                exchange_config=cls.exchange_config,
                routing_key='job.test')

        cls.retry_job_queue = PublishQueue(
                sender='unittester',
                connection_config=cls.connection_config,
                exchange_config=cls.retry_exchange_config,
                routing_key='job.retry')

        cls.retry_job_wait_queue = ConsumeQueue(
                connection_config=cls.connection_config,
                exchange_config=cls.retry_exchange_config,
                queue_config=cls.retry_queue_config,
                routing_keys=['job.*'])

        cls.job_queue = ConsumeQueue(
                connection_config=cls.connection_config,
                exchange_config=cls.exchange_config,
                queue_config=cls.queue_config,
                routing_keys=['job.*'],
                retry_queue=cls.retry_job_queue)
        
        #create retry job wait queue put make sure no consumers are
        #connected to it.  the idea is that failed messages will
        #be sent this queue and timeout after x-message-ttl milliseconds
        #and be resent to the 'unittest_job' exchange.
        cls.retry_job_wait_queue.start()
        gevent.sleep(1)
        cls.retry_job_wait_queue.stop()
        cls.retry_job_wait_queue.join()

        cls.create_job_queue.start()
        cls.retry_job_queue.start()
        cls.job_queue.start()

        #give time for connections to be established
        gevent.sleep(1)
    
    @classmethod
    def tearDownClass(cls):
        cls.create_job_queue.stop()
        cls.retry_job_queue.stop()
        cls.job_queue.stop()

        cls.create_job_queue.join()
        cls.retry_job_queue.join()
        cls.job_queue.join()

    def test_retry(self):
        self.create_job_queue.put('myjob')
        
        with self.assertRaises(Exception):
            with self.job_queue.get() as msg:
                self.assertEqual('myjob', msg.body)
                raise Exception('nack')
        
        with self.job_queue.get(True, 2) as msg:
            self.assertEqual('job.test', msg.header.routing_key)
            self.assertEqual('myjob', msg.body)

if __name__ == "__main__":
    unittest.main()
