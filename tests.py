import logging
import logging.config
import os
import time
import unittest

from docker import Client

from daemon import jenkins, stand, stand_manager
from daemon.config import DaemonConfig
from daemon.exceptions import DaemonException
from daemon.stand_db import StandPostgresDb, StandMssqlDb

log = logging.getLogger(__name__)


class DaemonTests(unittest.TestCase):
    def setUp(self):
        config = DaemonConfig().load_default()
        test_id = str(time.time())

        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_dir', 'test_{0}'.format(test_id))
        os.mkdir(self.test_dir)

        self.DB_IP = '192.168.201.187'

        config.work_dir = self.test_dir
        config.max_active_stands = 1
        config.ports = 5
        config.start_port = 8500
        config.postgres_backup_dir = self.test_dir
        config.postgres_addr = self.DB_IP
        config.db_prefix = 't{}_'.format(test_id[-6:])

        config.log_level = logging.DEBUG
        log_conf = config.default_logging()
        logging.config.dictConfig(log_conf)

        self.config = config

        # Test constants
        self.NAME = 'unittest'
        self.DESCRIPTION = 'conatainer for test, delme'
        self.PORTS = [8500, 8501]
        self.PROJECT = 'product_uni'
        self.MSSQL = '192.168.201.5'
        self.LAST_BUILD_DB_NAME = 'docker_last_build'

        self.EXISTED_STAND_DIR = '/home/okleptsova/PycharmProjects/stand-daemon/test_container'
        self.EXISTED_BUILD_DB_NAME = 'docker_old_build'
        self.EXISTED_STAND_DETAILS = {'image': config.image,
                                      'container_id': None,
                                      'name': self.NAME,
                                      'description': self.DESCRIPTION,
                                      'ports': self.PORTS,
                                      'docker_url': config.docker_url,
                                      'stand_dir': self.EXISTED_STAND_DIR,

                                      'db_type': 'postgres',
                                      'db_addr': self.DB_IP,
                                      'db_port': 5432,
                                      'db_name': self.EXISTED_BUILD_DB_NAME,
                                      'db_user': config.postgres_user,
                                      'db_pass': config.postgres_pass,
                                      'backup_dir': self.test_dir,
                                      'last_backup': None,
                                      'validate_entity_code': True,
                                      'uni_schema': None,

                                      'jenkins_project': self.PROJECT,
                                      'jenkins_version': None,
                                      'jenkins_url': config.jenkins_url,
                                      'jenkins_user': config.jenkins_user,
                                      'jenkins_pass': config.jenkins_pass,
                                      'version': None,
                                      'catalina_opt': config.catalina_opt,

                                      'active_task': None,
                                      }
        self.SECOND_CONTAINER = 'unittest2'

        cli = Client(base_url=self.config.docker_url)
        for c in (self.NAME, self.SECOND_CONTAINER):
            try:
                cli.remove_container(c, v=True, force=True)
            except Exception:
                pass

    def test_1_jenkins(self):
        """
        Собирает и скачивает сборки определенной версии
        """
        project = 'unipgups_branchTest'
        version = 'tandem_uni_v_2_10_5_2016_07_25'
        j = jenkins.Jenkins(self.config.jenkins_url, self.config.jenkins_user, self.config.jenkins_pass)
        build = j.build_project(project, version)
        j.get_build(project, self.test_dir, build_number=build)
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, 'WEB-INF')))
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, 'META-INF')))

    def test_2_config(self):
        """
        Создание hibernate.properties файла
        """
        s = stand.Stand(**self.EXISTED_STAND_DETAILS)
        s.stand_dir = self.test_dir
        config_dir = os.path.join(self.test_dir, 'config')
        os.mkdir(config_dir)
        s._create_hibernate_properties(pattern=self.config.postgres_hibernate_config)
        file = os.path.join(config_dir, 'hibernate.properties')
        self.assertTrue(os.path.exists(file))
        with open(file, 'r') as f:
            self.assertEqual('hibernate.dialect org.hibernate.dialect.PostgreSQLDialect\n'
                             'hibernate.connection.driver_class org.postgresql.Driver\n'
                             'hibernate.connection.url jdbc:postgresql://{0}:{1}/{2}\n'
                             'hibernate.connection.username {3}\n'
                             'hibernate.connection.password {4}\n'.format(self.DB_IP,
                                                                          5432,
                                                                          self.EXISTED_BUILD_DB_NAME,
                                                                          self.config.postgres_user,
                                                                          self.config.postgres_pass),
                             f.read())

    def test_3_container(self):
        """
        Создание, запуск, остановка и удаление докер-контейнера
        """
        container = stand.Stand(**self.EXISTED_STAND_DETAILS)
        container.create_container()
        container.start()
        time.sleep(20)
        container.check_http(180)
        container.stop()
        with self.assertRaises(Exception):
            container.check_http(10)
        container.remove()

    def test_4_free_resources(self):
        """
        Отказ в запуске стенда при исчерпании ресурсов
        """
        sm = stand_manager.StandManager(self.config)

        self.assertTrue(sm.free_resources())
        some_stand = stand.Stand(**self.EXISTED_STAND_DETAILS)
        some_stand.create_container()
        sm.stands['something_1'] = some_stand
        sm.stands['something_2'] = stand.Stand(**self.EXISTED_STAND_DETAILS)
        sm.start('something_1')
        time.sleep(20)
        with self.assertRaises(DaemonException):
            sm.start('something_2')
        some_stand.remove()
        self.assertTrue(sm.free_resources())

    def test_5_stand_no_new_build(self):
        """
        Сборка стенда из собранного билда. Стенд должен запускаться. Должна быть информация о стенде.
        Должны быть доступны логи стенда.
        """
        sm = stand_manager.StandManager(self.config)
        t = sm.add_new(name=self.NAME,
                       db_type='postgres',
                       description=self.DESCRIPTION,
                       jenkins_project=self.PROJECT,
                       db_name=self.LAST_BUILD_DB_NAME,
                       existed_db=True,
                       do_build=False)
        t.run(no_exceptions=False)

        t.stand.start()
        time.sleep(20)
        t.stand.check_http()
        expected = {'status': 'running',
                    'url': 'http://{0}:{1}/'.format(self.config.uni_docker_url, self.PORTS[0])}
        actual = sm.get_stands(full_info=False, active_only=True)
        for pair in expected.items():
            self.assertIn(pair, actual[self.NAME].items())
        self.assertIsNotNone(actual[self.NAME]['version'])

        stand_log = sm.catalina_out(self.NAME)
        self.assertNotEqual(str(stand_log).find('Server startup in'), -1)

    def test_51_error_without_existed_db_flag(self):
        """
        Без указания параметра existed_db=True должен выдать ошибку при попытке подключиться к существующей баз данных
        """
        sm = stand_manager.StandManager(self.config)
        t = sm.add_new(name=self.NAME,
                       db_type='postgres',
                       description=self.DESCRIPTION,
                       jenkins_project=self.PROJECT,
                       db_name=self.LAST_BUILD_DB_NAME,
                       do_build=False)
        with self.assertRaises(Exception):
            t.run(no_exceptions=False)

    def test_6_update_stand(self):
        """
        Создание и обновление стенда. Стенд должен запускаться. Должна быть информация о стенде
        """

        sm = stand_manager.StandManager(self.config)
        t = sm.add_new(name=self.NAME,
                       db_type='postgres',
                       description=self.DESCRIPTION,
                       jenkins_project=self.PROJECT,
                       db_name=self.LAST_BUILD_DB_NAME,
                       existed_db=True,
                       do_build=False)
        t.run(no_exceptions=False)

        t = sm.update(self.NAME)
        t.run(no_exceptions=False)

        expected = {'jenkins_project': self.PROJECT,
                    'url': 'http://{0}:{1}/'.format(self.config.uni_docker_url, self.PORTS[0]),
                    'db_type': 'postgres',
                    'status': 'stopped',
                    'db_port': 5432,
                    'db_name': self.LAST_BUILD_DB_NAME,
                    'jenkins_version': None,
                    'db_addr': self.DB_IP,
                    'description': self.DESCRIPTION,
                    'debug_port': self.PORTS[1]}
        actual = sm.get_stands(full_info=True, active_only=False)
        for pair in expected.items():
            self.assertIn(pair, actual[self.NAME].items())
        self.assertIsNotNone(actual[self.NAME]['version'])

    def test_7_ports(self):
        """
        Выдача портов из диапазона. Отказ в выдаче при исчерпании диапазона.
        """
        sm = stand_manager.StandManager(self.config)
        sm.add_new('1', '1', '1', '1')
        t = sm.add_new('2', '2', '2', '2')
        self.assertEqual([self.config.start_port + 2, self.config.start_port + 3],
                         t.stand.ports)
        with self.assertRaises(DaemonException):
            sm.add_new('3', '3', '3', '3')

    def test_8_stands_from_json(self):
        """
        Создание объектов-контейнеров имеющихся стендов при запуске
        """
        sm = stand_manager.StandManager(self.config)
        sm.add_new('name', 'postgres', 'jenkins_project')
        sm2 = stand_manager.StandManager(self.config)

        expected = {'db_type': 'postgres', 'jenkins_project': 'jenkins_project'}
        actual = sm2.get_stands(full_info=True, active_only=False, task_only=False)

        for pair in expected.items():
            self.assertIn(pair, actual['name'].items())

    def test_10_db_postgres(self):
        """
        Cоздание, бэкап и восстановление баз данных Postgres
        """
        backup_path = os.path.join(self.test_dir, 'test.backup')
        db = StandPostgresDb(addr=self.DB_IP,
                             port=5432,
                             name='test{}'.format(time.time()).replace('.', ''),
                             user=self.config.postgres_user,
                             password=self.config.postgres_pass)
        db.create()
        db.backup(backup_path)
        db.restore(backup_path)

    def test_11_db_mssql(self):
        """
        Cоздание, бэкап и восстановление баз данных MSSQL
        """
        backup_path = '{}\\test.bak'.format(self.config.mssql_backup_dir)
        db = StandMssqlDb(addr=self.MSSQL,
                          name='test{}'.format(time.time()).replace('.', ''),
                          user=self.config.mssql_user,
                          password=self.config.mssql_pass)
        db.create()
        db.backup(backup_path)
        db.restore(backup_path)

    def _stand_and_new_db(self, db_type):
        sm = stand_manager.StandManager(self.config)
        t = sm.add_new(name=self.NAME,
                       db_type=db_type,
                       description=self.DESCRIPTION,
                       jenkins_project=self.PROJECT)
        t.run(no_exceptions=False)

        stand_name = t.stand.name
        stand = t.stand

        sm.start(stand_name)
        time.sleep(20)
        stand.check_http()
        stand.stop(wait=True)
        stand.db.reduce()

        sm.backup_db(stand_name).run(no_exceptions=False)
        sm.start(stand_name)
        time.sleep(20)
        stand.check_http()

        sm.restore_db(stand_name).run(no_exceptions=False)

        sm.start(stand_name)
        time.sleep(20)
        t.stand.check_http()

    def test_12_stand_and_new_db_mssql(self):
        """
        Создание бд во время создания стенда. Новая база будет заполнена.
        Удаление блобов из новой базы
        Бэкап и восстановление баз данных стенда через менеджера
        """
        self._stand_and_new_db('mssql')

    def test_12_stand_and_new_db_postgres(self):
        """
        Создание бд во время создания стенда. Новая база будет заполнена.
        Удаление блобов из новой базы
        Бэкап и восстановление баз данных стенда через менеджера
        """
        self._stand_and_new_db('postgres')

    def test_13_clone(self):
        sm = stand_manager.StandManager(self.config)
        sm.add_new(name=self.NAME,
                   db_type='postgres',
                   description=self.DESCRIPTION,
                   jenkins_project=self.PROJECT,
                   do_build=False).run(no_exceptions=False)

        task_list = sm.clone(self.NAME, self.SECOND_CONTAINER, do_backup=True)
        for t in task_list:
            t.run(no_exceptions=False)

        info = sm.get_stands(full_info=True)

        the_same = ['version', 'db_type', 'db_addr', 'db_port', 'jenkins_project', 'jenkins_version']
        different = ['url', 'debug_port', 'db_name', 'description']

        for param in the_same:
            self.assertEqual(info[self.NAME][param], info[self.SECOND_CONTAINER][param])

        for param in different:
            self.assertNotEqual(info[self.NAME][param], info[self.SECOND_CONTAINER][param])

        self.assertEqual(str(sm.catalina_out(self.SECOND_CONTAINER)).find('New database'), -1)
