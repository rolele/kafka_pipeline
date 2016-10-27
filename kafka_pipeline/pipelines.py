# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.utils.serialize import ScrapyJSONEncoder

from twisted.internet import defer
from kafka import KafkaClient, SimpleProducer
from kafka.common import KafkaUnavailableError
import json
import datetime as dt
import sys
import traceback
import base64
from twisted.internet.error import ConnectError
import traceback
class KafkaPipeline(object):
    '''
    Pushes a serialized item to appropriate Kafka topics.
    '''

    def __init__(self, producer, aKafka):
        self.producer = producer
        self.kafka = aKafka

    @classmethod
    def from_settings(cls, settings):
        kafka = KafkaClient("kafka:9092")
        producer = SimpleProducer(kafka)
        return cls(producer, kafka)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        try:
            spider.logger.debug("Processing item in KafkaPipeline")
            datum = dict(item)
            spider.logger.debug(datum)
            message = "default"
            try:
                message = json.dumps(datum)
            except:
                spider.logger.debug(traceback.format_exc())
                raise

            topic = "kafkapipeline"
            self.kafka.ensure_topic_exists(topic)
            spider.logger.debug(message)
            self.producer.send_messages(topic, message.encode('utf-8'))
            spider.logger.debug("Item processed in KafkaPipeline")
            yield self.producer.send_messages(topic, message)
        except:
            spider.logger.debug(traceback.format_exc())
            raise
        finally:
            defer.returnValue(item)
