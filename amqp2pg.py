#!/usr/bin/env python
# encoding: utf-8

from cStringIO import StringIO
from threading import Thread, Lock
from time import time, sleep
import argparse
import csv
import logging
import sys

import psycopg2
import pika


logger = logging.getLogger(__name__)


class Shared(object):

    def __init__(self):
        self.messages = []
        self.last_tag = None
        self.lock = Lock()
        self.to_ack = []
        self.log_time = time()
        self.log_count = 0

    def on_message(self, ch, frame, header, body):

        t = time()
        if t - self.log_time > 1.0 and self.log_count:
            logger.debug("Got %d messages in %d sec", self.log_count, t - self.log_time)
            self.log_time = t
            self.log_count = 0

        with self.lock:
            self.messages.append((body, frame.delivery_tag))

    def kick_acks(self, ch):
        with self.lock:
            to_ack, shared.to_ack = shared.to_ack, []
        for tag in to_ack:
            ch.basic_ack(delivery_tag=tag, multiple=True)

    def get_all(self):
        with self.lock:
            msgs, self.messages = self.messages, []
        return msgs

    def done(self, tag):
        with self.lock:
            self.to_ack.append(tag)


shared = Shared()


def consumer():
    while True:
        try:

            def on_ch_open(ch):

                #ch.basic_qos(logger.debug, prefetch_count=args.max_size * 2)

                ch.queue_declare(logger.debug, queue=args.queue, durable=True)

                def kick_acks():
                    shared.kick_acks(ch)
                    conn.ioloop.add_timeout(1., kick_acks)

                conn.ioloop.add_timeout(1., kick_acks)

                if args.rq_bind:
                    exch, routing_key = args.rq_bind
                    ch.queue_bind(queue=args.queue, exchange=exch, routing_key=routing_key)

                ch.basic_consume(shared.on_message, queue=args.queue)

            conn = pika.SelectConnection(pika.ConnectionParameters(
                args.rq_host, args.rq_port, args.rq_vhost,
                credentials=pika.PlainCredentials(args.rq_user, args.rq_passwd),
            ), on_open_callback=lambda conn: conn.channel(on_open_callback=on_ch_open))
            conn.ioloop.start()
        except:
            logger.error("Consumer thread got unexpected exception:", exc_info=True)
            conn.close()
            conn.ioloop.start()
            sleep(5.)


def saver():

    while True:

        t0 = time()
        msgs = shared.get_all()
        logger.debug("Got %d messages", len(msgs))

        if msgs:

            buf = StringIO()
            w = csv.writer(buf)

            for i in range(0, len(msgs), args.max_size):
                chunk = msgs[i:i + args.max_size]
                columns, data = converter.convert_raws([row for row, tag in chunk])
                w.writerows(data)
                buf.seek(0)
                c = db.cursor()
                c.copy_expert("COPY {table} ({columns}) FROM STDIN CSV".format(
                    table=args.table,
                    columns=', '.join(columns),
                ), buf)
                c.close()
                db.commit()
                shared.done(chunk[-1][1])
                logger.info("Wrote %d rows in %.3f sec.", len(data), time() - t0)

        next_time = t0 + 0.5
        dt = next_time - time()
        if dt > 0.:
            sleep(dt)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--converter", required=True, help=u"Какой конвертер использовать")
    parser.add_argument("--converters-pkg", default='amqp2pg_conv', help=u"Имя пакета с конвертерами")
    parser.add_argument("--database", required=True, help=u"Имя БД")
    parser.add_argument("--table", required=True, help=u"Имя таблицы")
    parser.add_argument("--queue", required=True, help=u"RabbitMQ queue to consume")
    parser.add_argument("--max-size", default=5000, type=int,
                        help=u"Max messages count to write in single transaction")

    db_args = parser.add_argument_group("Database", u"Параметры базы данных")
    db_args.add_argument("--db-host", help=u"Хост")
    db_args.add_argument("--db-port", type=int, help=u"Порт")
    db_args.add_argument("--db-user", help=u"Пользователь")
    db_args.add_argument("--db-passwd", help=u"Пароль")

    rq_args = parser.add_argument_group(
        u"RabbitMQ",
        u"Параметры подключения к RabbitMQ для отправки сообщений"
    )
    rq_args.add_argument("--rq-host", default="127.0.0.1",           help=u"RabbitMQ host")
    rq_args.add_argument("--rq-port", default=5672,                  help=u"RabbitMQ port")
    rq_args.add_argument("--rq-user", default="guest",               help=u"RabbitMQ user")
    rq_args.add_argument("--rq-passwd", default="guest",             help=u"RabbitMQ password")
    rq_args.add_argument("--rq-vhost", default="/",                  help=u"RabbitMQ virtual host")

    rq_args.add_argument("--rq-bind", nargs=2, metavar=('EXCHANGE', 'ROUTING_KEY'),
                         help=u"Bind queue to the specified exchange (optional)")

    parser.add_argument("--log-file", help=u"Имя лог файла (если не указано - логи пишутся в stderr)")
    parser.add_argument("--log-level", default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help=u"Уровень логгирования")

    parser.add_argument("--syslog", help=u"Хост куда отправлять логи Syslog")
    parser.add_argument("--syslog-level", default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help=u"Уровень логгирования в Syslog")

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)

    root_logger = logging.getLogger()

    if args.log_file:
        log_handler = logging.handlers.TimedRotatingFileHandler(args.log_file)
        log_handler.setFormatter(logging.Formatter(
            u'%(asctime)s %(levelname)8s %(funcName)s:%(lineno)d %(message)s'
        ))
        log_handler.setLevel(log_level)
        root_logger.addHandler(log_handler)
        root_logger.setLevel(log_level)
    else:
        logging.basicConfig(
            format=u'%(asctime)s.%(msecs).3d %(levelname)8s %(module)6s:%(lineno)03d %(message)s',
            level=log_level, datefmt="%m.%d %H:%M:%S"
        )

    if args.syslog:

        syslog_level = getattr(logging, args.syslog_level)

        syslog_handler = logging.handlers.SysLogHandler((args.syslog, 514))
        syslog_handler.socket.setblocking(False)
        syslog_handler.setFormatter(logging.Formatter(
            sys.argv[0] + u'[%(process)s]: %(levelname)8s %(module)s:%(lineno)d %(message)s'
        ))
        syslog_handler.setLevel(syslog_level)

        root_logger.addHandler(syslog_handler)

        syslog_logger = logging.getLogger('syslog')
        syslog_logger.propagate = False
        syslog_logger.addHandler(syslog_handler)
        syslog_logger.setLevel(syslog_level)

    db = psycopg2.connect(host=args.db_host, port=args.db_port,
                          database=args.database,
                          user=args.db_user, password=args.db_passwd)

    converter = getattr(__import__(args.converters_pkg), args.converter).convert

    consumer_thread = Thread(target=consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    saver_thread = Thread(target=saver)
    saver_thread.daemon = True
    saver_thread.start()

    saver_thread.join()
