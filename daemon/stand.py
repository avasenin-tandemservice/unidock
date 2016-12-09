import json
import logging
import os
import shutil
import time

from docker import Client, errors
from tornado import gen
from tornado.httpclient import HTTPClient, AsyncHTTPClient, HTTPError

from daemon.exceptions import DaemonException, InvalidStandInfo
from daemon.stand_db import StandMssqlDb, StandPostgresDb, StandDockerPostgres

log = logging.getLogger(__name__)


class Stand:
    def __init__(self, **kwargs):
        log.debug('Initialize new stand container object')
        try:
            self.image = kwargs['image']
            self.catalina_opt = kwargs['catalina_opt']

            self.container_id = kwargs['container_id']
            self.name = kwargs['name']
            self.description = kwargs['description']
            self.stand_dir = kwargs['stand_dir']
            self.ports = kwargs['ports']

            self.db_type = kwargs['db_type']
            self.db_addr = kwargs['db_addr']
            self.db_port = kwargs['db_port']
            self.db_name = kwargs['db_name']
            self.db_user = kwargs['db_user']
            self.db_pass = kwargs['db_pass']

            self.db_container = kwargs['db_container']
            self.ssh_user = kwargs['ssh_user']
            self.ssh_pass = kwargs['ssh_pass']

            self.last_backup = kwargs['last_backup']
            self.backup_dir = kwargs['backup_dir']

            # Для клиентов ведущих активную разработку в своем слое отключаем валидации
            self.validate_entity_code = kwargs['validate_entity_code']

            # В mssql нельзя назначить схему отличную от dbo для пользователя
            # обладающего правами sysadmin, которые в свою очередь нужны для других операции с бд
            # в проекте usue схема uni
            self.uni_schema = kwargs['uni_schema']

            self.jenkins_project = kwargs['jenkins_project']
            self.jenkins_version = kwargs['jenkins_version']
            self.jenkins_url = kwargs['jenkins_url']
            self.jenkins_user = kwargs['jenkins_user']
            self.jenkins_pass = kwargs['jenkins_pass']
            self.version = kwargs['version']

            self.active_task = kwargs['active_task']
            self.web_interface_error = kwargs['web_interface_error']
        except KeyError:
            raise InvalidStandInfo()

        self.cli = Client(base_url='unix://var/run/docker.sock')
        self.stand_info = os.path.join(self.stand_dir, 'stand_info.json')

        if self.db_type == 'postgres':
            self.db = StandPostgresDb(self.db_addr, self.db_name, self.db_user, self.db_pass, self.db_port)
        elif self.db_type == 'mssql':
            self.db = StandMssqlDb(self.db_addr, self.db_name, self.db_user, self.db_pass, self.db_port)
        elif self.db_type == 'pgdocker':
            self.db = StandDockerPostgres(self.db_addr, container_name=self.db_container, ssh_user=self.ssh_user,
                                          ssh_password=self.ssh_pass, port=self.db_port)
        else:
            raise RuntimeError('Unsupported database type')

        if not os.path.isdir(self.stand_dir):
            os.mkdir(self.stand_dir)

        if not os.path.exists(self.stand_info):
            open(self.stand_info, 'x').close()

        self.write_json()

    def write_json(self):
        d = {}
        for key, val in self.__dict__.items():
            if key in ('cli', 'stand_info', 'db'):
                continue
            d[key] = val

        log.debug('dump stand info %s', d)

        with open(self.stand_info, 'wt') as f:
            json.dump(d, f)

    def _create_hibernate_properties(self, pattern):
        log.debug('Create hibernate file for %s', self.name)
        with open(pattern) as f:
            conf = f.read()

        conf = conf.format(addr=self.db_addr, name=self.db_name, port=self.db_port,
                           user=self.db_user, password=self.db_pass)

        if not self.validate_entity_code:
            conf += '\ndb.validateEntityCode=false\n'

        with open(os.path.join(self.stand_dir, 'config', 'hibernate.properties'), 'wt') as f:
            f.write(conf)

    def create_dir_structure(self, config_dir, pattern):
        log.debug('Create files for %s', self.name)
        stand_config_dir = os.path.join(self.stand_dir, 'config')
        shutil.copytree(config_dir, stand_config_dir)
        self._create_hibernate_properties(pattern)
        os.mkdir(os.path.join(self.stand_dir, 'webapp'))

    def create_container(self):
        log.info('Create container. Image: %s, dir: %s, ports: %s', self.image, self.stand_dir, self.ports)
        log.debug('%s use docker create_container', self.name)
        try:
            container_id = self.cli.create_container(image=self.image,
                                                     name=self.name,
                                                     volumes=['/usr/local/uni'],
                                                     ports=[8080, 8180],
                                                     host_config=self.cli.create_host_config(
                                                             binds=['{0}:/usr/local/uni'.format(self.stand_dir)],
                                                             port_bindings={8080: self.ports[0],
                                                                            8180: self.ports[1]}),
                                                     environment={'CATALINA_OPTS': self.catalina_opt,
                                                                  'TZ': 'Asia/Yekaterinburg'},
                                                     )
        except (errors.DockerException, errors.APIError) as e:
            raise DaemonException(str(e))

        self.container_id = container_id['Id']
        self.write_json()
        return self.container_id

    def start(self, wait=True):
        log.info('Start container %s', self.name)

        if not self.container_id:
            raise DaemonException('Container is not exists')
        if self.db_type == 'pgdocker':
            self.db.start()
        try:
            self.cli.start(self.container_id)
        except (errors.DockerException, errors.APIError) as e:
            raise DaemonException(str(e))
        self._check_uni_iface(blocking=wait)

    def is_running(self):
        if not self.container_id:
            return False
        else:
            try:
                return self.cli.inspect_container(self.container_id)['State']['Running']
            except (errors.DockerException, errors.APIError):
                return False

    @gen.coroutine
    def _check_uni_iface(self, blocking=True, timeout=900):
        log.debug('Check connect to %s container', self.name)

        # Томкат начинает слушать порт не сразу. Игнорируем ошибки пока не истек таймаут
        # Если контейнер не запущен, то веб-интерфейс не будет доступен
        deadline = time.time() + timeout
        while 1:
            if not self.is_running():
                self.web_interface_error = 'Container has unexpectedly stopped'
                break
            try:
                if blocking:
                    cl = HTTPClient()
                    cl.fetch('http://localhost:{0}/'.format(self.ports[0]), request_timeout=timeout)
                else:
                    cl = AsyncHTTPClient()
                    yield cl.fetch('http://localhost:{0}/'.format(self.ports[0]), request_timeout=timeout)

                self.web_interface_error = None
                break
            except (ConnectionError, HTTPError) as e:
                if time.time() < deadline:
                    if blocking:
                        time.sleep(5)
                    else:
                        yield gen.sleep(5)
                else:
                    self.web_interface_error = str(e)
                    break
            finally:
                cl.close()

        if self.web_interface_error:
            log.info('Container %s is not available. Web interface error: %s', self.name, self.web_interface_error)
        else:
            log.info('Container %s is available', self.name)

        self.write_json()

    def stop(self, wait=True):
        log.info('Stop container %s', self.name)
        if not self.container_id:
            raise DaemonException('Container is not exists')
        try:
            self.cli.stop(self.container_id, timeout=60)
        except (errors.DockerException, errors.APIError) as e:
            raise DaemonException(str(e))
        if wait:
            self.cli.wait(self.container_id)

    def remove(self):
        self.stop()
        log.info('Remove container %s', self.name)
        try:
            self.cli.remove_container(self.container_id, v=True)
        except (errors.DockerException, errors.APIError) as e:
            raise DaemonException(str(e))
        self.container_id = None
        self.write_json()
