#!/usr/bin/python3

import logging
import logging.config
from concurrent.futures import ThreadPoolExecutor

from tornado.ioloop import IOLoop
from tornado.web import Application

import web_handlers
from daemon.config import DaemonConfig
from daemon.stand_manager import StandManager


def main():
    conf = DaemonConfig().load_default()
    logging.config.dictConfig(conf.default_logging())

    application = Application([
        (r'/stand/([a-z,0-9,\-,_]+)/*([a-z]*)', web_handlers.StandHandler),
        (r'/list/*', web_handlers.ListHandler),
        (r'/.*', web_handlers.HelpHandler),
    ])

    # tpe для коротких и длинных тасков. Длинные таски не должны весить сервер, потому выполняются по одному.
    # кроме того, не тестировалась параллельная сборка на дженкинсе
    application.fast_task_tpe = ThreadPoolExecutor(max_workers=8)
    application.long_task_tpe = ThreadPoolExecutor(max_workers=1)
    application.conf = conf

    sm = StandManager(conf)
    for t in sm.uncompleted_tasks:
        application.long_task_tpe.submit(t.run)
    application.sm = sm

    application.listen(conf.uni_docker_port)
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
