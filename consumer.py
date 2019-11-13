import configparser
import pika
import os
import logger
import time

logger = logger.ini_logger(__name__)


def connect(host, queue):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue)

    return connection, channel


def consume(host, queue, sleep_timer):
    connection, channel = connect(host, queue)

    while True:
        method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
        if not body:
            channel.close()
            connection.close()
            time.sleep(sleep_timer)
            connection, channel = connect(host, queue)
        else:
            logger.info(body)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config_dir = os.path.abspath(os.path.dirname(__file__))
    config.read(config_dir + f"{os.sep}config.ini")

    consume(config['connection']['host'], config['connection']['queue'], float(config['sleep']['timeout']))

