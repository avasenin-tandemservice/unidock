import logging
import os

from configparser import ConfigParser, NoOptionError


class DaemonConfig(object):
    def __init__(self):
        # Общие настройки
        self.uni_docker_url = 'undefined'
        self.uni_docker_port = -1
        self.log_level = 'undefined'

        # Директория куда сохранять стенды и логи
        self.work_dir = 'undefined'

        # Управление ресурсами
        self.max_active_stands = -1
        self.start_port = -1
        self.ports = -1
        self.stop_by_timeout = True

        # Базы данных, таймауты в секундах
        self.backup_timeout = -1
        self.restore_timeout = -1

        # Значения по умолчанию для создания НОВЫХ стендов
        self.config_dir = 'undefined'
        self.image = 'undefined'
        self.catalina_opt = 'undefined'
        self.db_prefix = 'undefined'

        # шаблон конфига
        self.postgres_addr = 'undefined'
        self.postgres_hibernate_config = 'undefined'
        self.postgres_user = 'undefined'
        self.postgres_pass = 'undefined'
        self.postgres_backup_dir = 'undefined'
        self.postgres_ignore_restore_errors = True

        self.pgdocker_start_port = -1
        self.pgdocker_ports = -1
        self.pgdocker_use_ssh = False
        self.pgdocker_addr = 'undefined'
        self.pgdocker_ssh_user = 'undefined'
        self.pgdocker_ssh_pass = 'undefined'
        self.pgdocker_backup_dir = 'undefined'

        self.mssql_addr = 'undefined'
        self.mssql_hibernate_config = 'undefined'
        self.mssql_user = 'undefined'
        self.mssql_pass = 'undefined'
        self.mssql_backup_dir = 'undefined'
        self.mssql_db_dir = 'undefined'

        self.jenkins_url = 'undefined'
        self.jenkins_user = 'undefined'
        self.jenkins_pass = 'undefined'

        self.defined = False

    def default_logging(self):
        if not self.defined:
            raise RuntimeError('You should define settings before this action')

        return {
            'version': 1,
            'formatters': {
                'main_formatter': {'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'},
            },
            'handlers': {
                'console': {'class': 'logging.StreamHandler', 'formatter': 'main_formatter'},
                'file': {'class': 'logging.FileHandler', 'formatter': 'main_formatter',
                         'filename': os.path.join(self.work_dir, 'log.txt')},
                'tornado_access_file': {'class': 'logging.FileHandler', 'formatter': 'main_formatter',
                                        'filename': os.path.join(self.work_dir, 'tornado_access.txt')},
            },
            'loggers': {
                'tornado.application': {'level': logging.ERROR, 'handlers': ['console', 'file']},
                'tornado.access': {'level': logging.INFO, 'handlers': ['tornado_access_file']},
                'daemon.stand_manager': {'handlers': ['console', 'file']},
                'daemon.task': {'handlers': ['console', 'file']},
                'daemon.jenkins': {'handlers': ['console', 'file']},
                'daemon.stand': {'handlers': ['console', 'file']},
                'daemon.stand_db': {'handlers': ['console', 'file']},
                'web_handlers': {'handlers': ['console', 'file']},
                'service': {'handlers': ['console', 'file']},
            },
            'root': {
                'level': self.log_level,
            },
        }

    def load_default(self):
        with open(os.path.join(os.path.dirname(__file__), 'default_config.txt')) as f:
            config = ConfigParser()
            config.read_file(f)
        for obj_name, obj_val in self.__dict__.items():
            if obj_name == 'defined':
                continue
            try:
                if type(obj_val) is bool:
                    val = config.getboolean(section='all', option=obj_name)
                elif type(obj_val) is int:
                    val = config.getint(section='all', option=obj_name)
                else:
                    val = config.get(section='all', option=obj_name)
            except NoOptionError:
                raise RuntimeError('Config param %s is missed' % obj_name)

            self.__dict__[obj_name] = val

        self.defined = True

        return self
