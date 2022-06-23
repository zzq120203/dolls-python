import asyncio
from typing import Callable, Any, Coroutine, Optional

import pulsar


class PProducer(object):
    def __init__(
            self,
            topic: str,
            schema=pulsar.schema.StringSchema(),
            client: pulsar.Client = None,
            url: str = None,
            auth=None,
            **kwargs
    ) -> None:
        if client is None:
            if not url.startswith("pulsar://"):
                url = "pulsar://" + url
            client = pulsar.Client(service_url=url, authentication=auth)

        self.__producer = client.create_producer(
            topic=topic,
            producer_name=kwargs.get("producer_name", None),
            schema=schema,
            initial_sequence_id=kwargs.get("initial_sequence_id", None),
            send_timeout_millis=kwargs.get("send_timeout_millis", 30000),
            compression_type=kwargs.get("compression_type", pulsar.CompressionType.NONE),
            max_pending_messages=kwargs.get("max_pending_messages", 1000),
            max_pending_messages_across_partitions=kwargs.get("max_pending_messages_across_partitions", 50000),
            block_if_queue_full=kwargs.get("block_if_queue_full", False),
            batching_enabled=kwargs.get("batching_enabled", False),
            batching_max_messages=kwargs.get("batching_max_messages", 1000),
            batching_max_allowed_size_in_bytes=kwargs.get("batching_max_allowed_size_in_bytes", 128 * 1024),
            batching_max_publish_delay_ms=kwargs.get("batching_max_publish_delay_ms", 10),
            message_routing_mode=kwargs.get("message_routing_mode",
                                            pulsar.PartitionsRoutingMode.RoundRobinDistribution),
            properties=kwargs.get("properties", None),
        )

    # 生产数据
    def send(self, data, sync: bool = True):
        if sync:
            self.__producer.send(data)
        else:
            self.__producer.send_async(data, self.callback)

    # 释放资源
    def stop(self):
        self.__producer.close()

    # send callback
    @staticmethod
    def callback(result, msg):
        if str(result).upper() != "OK":
            print("Message({}) published: {}".format(msg, result))

    def test(self):
        self.send("message for test.", True)
        print("测试生产：message for test.")


class PConsumer(object):

    def __init__(
            self,
            topic: str,
            sub_name: str,
            schema=pulsar.schema.StringSchema(),
            client: pulsar.Client = None,
            url: str = None,
            auth=None,
            **kwargs
    ) -> None:

        if client is None:
            if not url.startswith("pulsar://"):
                url = "pulsar://" + url
            client = pulsar.Client(service_url=url, authentication=auth)

        self.__consumer = client.subscribe(
            topic=topic,
            subscription_name=sub_name,
            consumer_type=kwargs.get("consumer_type", pulsar.ConsumerType.Exclusive),
            schema=schema,
            message_listener=kwargs.get("consumer_type", None),
            receiver_queue_size=kwargs.get("consumer_type", 1000),
            max_total_receiver_queue_size_across_partitions=kwargs.get("consumer_type", 50000),
            consumer_name=kwargs.get("consumer_type", None),
            unacked_messages_timeout_ms=kwargs.get("consumer_type", None),
            broker_consumer_stats_cache_time_ms=kwargs.get("consumer_type", 30000),
            negative_ack_redelivery_delay_ms=kwargs.get("consumer_type", 60000),
            is_read_compacted=kwargs.get("consumer_type", False),
            properties=kwargs.get("consumer_type", None),
            pattern_auto_discovery_period=kwargs.get("consumer_type", 60),
            initial_position=kwargs.get("consumer_type", pulsar.InitialPosition.Latest)
        )

    # 消费数据
    def start(self, consumer_fun: Callable[[Any], bool]):
        while True:
            msg = self.__consumer.receive()
            data = msg.value()
            b = consumer_fun(data)
            if b:
                self.__consumer.acknowledge(msg)
            else:
                self.__consumer.negative_acknowledge(msg)

    # 消费数据
    async def async_start(self, consumer_fun: Callable[[Optional[Any]], Coroutine[Any, Any, bool]], *, timeout=0.01):
        while True:
            try:
                msg = self.__consumer.receive(timeout_millis=int(timeout * 1000))
                data = msg.value()
                b = await consumer_fun(data)
                if b:
                    self.__consumer.acknowledge(msg)
                else:
                    self.__consumer.negative_acknowledge(msg)
            except Exception as e:
                _ = await consumer_fun(None)
                # logger.warning(f"pulsar consumer err: {e}")
                await asyncio.sleep(0.01)

    def create_sub(self) -> bool:
        try:
            self.__consumer.receive(1)
        except Exception as e:
            return True
        return False

    # 释放资源
    def stop(self):
        self.__consumer.close()
