import json
import logging
import os
import shutil

import urllib3
from docker import Client, errors

from daemon.exceptions import DaemonException, InvalidStandInfo
from daemon.stand_db import StandMssqlDb, StandPostgresDb

log = logging.getLogger(__name__)


class Stand:
    def __init__(self, **kwargs):
        log.debug('Initialize new stand container object')
        try:
            self.docker_url = kwargs['docker_url']
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
            self.last_backup = kwargs['last_backup']

            # Набор костылей

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
        except KeyError:
            raise InvalidStandInfo()

        self.cli = Client(base_url=self.docker_url)
        self.stand_info = os.path.join(self.stand_dir, 'stand_info.json')

        if self.db_type == 'postgres':
            self.db = StandPostgresDb(self.db_addr, self.db_name, self.db_user, self.db_pass, self.db_port)
        elif self.db_type == 'mssql':
            self.db = StandMssqlDb(self.db_addr, self.db_name, self.db_user, self.db_pass, self.db_port)
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

    def start(self):
        log.info('Start container %s', self.name)

        if not self.container_id:
            raise DaemonException('Container is not exists')
        try:
            self.cli.start(self.container_id)
        except (errors.DockerException, errors.APIError) as e:
            raise DaemonException(str(e))

    def check_http(self, timeout=600):
        log.info('Check connect to %s container', self.name)
        conn = urllib3.connection_from_url('http://localhost:{0}/'.format(self.ports[0]))
        # urllib3.exceptions.MaxRetryError - исключение во время старта
        # urllib3.exceptions.ReadTimeoutError - таймаут
        conn.request('GET', '/', timeout=timeout)
        log.debug('Container %s is available', self.name)

    def stop(self, wait=True):
        log.info('Stop container %s', self.name)
        if not self.container_id:
            raise DaemonException('Container is not exists')
        try:
            self.cli.stop(self.container_id)
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
