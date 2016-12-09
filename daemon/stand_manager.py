import datetime
import json
import logging
import os
import socket

from docker import Client

from daemon import task
from daemon.config import DaemonConfig
from daemon.exceptions import DaemonException
from daemon.stand import Stand

log = logging.getLogger(__name__)


class StandManager:
    def __init__(self, config: DaemonConfig):
        log.info('Start stand manager')

        self.max_active_stands = config.max_active_stands
        self.work_dir = config.work_dir
        self.config_dir = config.config_dir
        self.uni_docker_url = config.uni_docker_url
        self.uni_docker_port = config.uni_docker_port
        self.start_port = config.start_port
        self.ports = config.ports
        self.stop_by_timeout = config.stop_by_timeout

        self.image = config.image
        self.catalina_opt = config.catalina_opt
        self.db_prefix = config.db_prefix

        self.postgres_hibernate_conf = config.postgres_hibernate_config
        self.postgres_addr = config.postgres_addr
        self.postgres_user = config.postgres_user
        self.postgres_pass = config.postgres_pass
        self.postgres_backup_dir = config.postgres_backup_dir

        self.pgdocker_start_port = config.pgdocker_start_port
        self.pgdocker_ports = config.pgdocker_ports
        self.pgdocker_addr = config.pgdocker_addr
        self.pgdocker_ssh_user = config.pgdocker_ssh_user
        self.pgdocker_ssh_pass = config.pgdocker_ssh_pass
        self.pgdocker_backup_dir = config.pgdocker_backup_dir

        self.mssql_addr = config.mssql_addr
        self.mssql_hibernate_conf = config.mssql_hibernate_config
        self.mssql_user = config.mssql_user
        self.mssql_pass = config.mssql_pass
        self.mssql_backup_dir = config.mssql_backup_dir

        self.jenkins_url = config.jenkins_url
        self.jenkins_user = config.jenkins_user
        self.jenkins_pass = config.jenkins_pass

        self.stands_dir = os.path.join(self.work_dir, 'stands')
        self.cli = Client(base_url='unix://var/run/docker.sock')

        # валидация рабочей директории
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        if not os.path.isdir(self.stands_dir):
            os.mkdir(self.stands_dir)
        if not os.path.isdir(self.postgres_backup_dir):
            os.mkdir(self.postgres_backup_dir)

        # создание объектов-контейнеров для имеющихся стендов
        """:type : dict[Stand]"""
        self.stands = {}
        #  Хранит футуры,которые должны выключить стенды
        self.stands_futures = {}
        # Собирает незавершенные таски найденные во время запуска
        self.uncompleted_tasks = []

        for stand_dir in os.listdir(self.stands_dir):
            stand_info_path = os.path.join(self.stands_dir, stand_dir, 'stand_info.json')
            if os.path.isfile(stand_info_path):
                self._init_stand(stand_info_path)

        log.info('Found containers: %s', ', '.join(self.stands.keys()))

    def _init_stand(self, stand_info_path):
        with open(stand_info_path, 'rt') as f:
            stand_info = json.loads(f.read())
        stand = Stand(**stand_info)
        self.stands[stand_info['name']] = stand

        active_task = stand_info['active_task']
        if active_task:
            log.info('Uncompleted task %s in %s', active_task, stand_info_path)

            # задачи которые успели дойти до тестового запуска продолжать нет смысла
            if active_task['status'] == task.TEST_RUN:
                stand.active_task = None
                stand.write_json()
                return

            # если задача закончилась с ошибкой, то ничего не делаем. инфа должна остаться
            if active_task['status'] == task.ERROR:
                return

            # все не начатые задачи продолжаем всегда
            if active_task['status'] == task.WAIT:
                t = task.Task(do=active_task['do'], stand=stand, **active_task['task_params'])
                self.uncompleted_tasks.append(t)
                return

            # задачи в статусе бидла эквиваленты обновлению
            if active_task['status'] == task.BUILD_AND_UPLOAD:
                t = task.Task(do=task.DO_UPDATE, stand=stand, **active_task['task_params'])
                self.uncompleted_tasks.append(t)
                return

            # если мы пытались восстановить базу данных, делаем все с начала
            if active_task['status'] == task.RESTORE_DB and active_task['do'] == task.DO_RESTORE:
                t = task.Task(do=task.RESTORE_DB, stand=stand, **active_task['task_params'])
                self.uncompleted_tasks.append(t)
                return

            # если мы пытались забэкапить базу данных, то бэкап надо удалить
            if active_task['status'] == task.DO_BACKUP:
                log.warning('Backup of stand %s is not completed and should be deleted', stand.name)
                # todo удаление бэкапа

            # если создание стенда не в статусе ожидания или тестового запуска, то лучше сделать все заново
            if active_task['do'] == task.DO_ADD_NEW and not active_task['task_params']['existed_db']:
                log.warning('Found wrong stand create. Stand %s should be deleted', stand.name)
                # todo удаление стенда
                return

            if active_task['do'] == task.DO_ADD_NEW and active_task['task_params']['existed_db']:
                log.warning('Found wrong stand create. Stand %s should be deleted', stand.name)
                # todo удаление стенда БЕЗ БАЗЫ
                return

            raise RuntimeError('Oops, what I should do with stand %s and task %s ?' % (stand.name, active_task))

    def _get_ports(self):
        used_ports = []
        for stand in self.stands.values():
            used_ports += stand.ports

        ports = []
        for port in range(self.start_port, self.start_port + self.ports):
            if len(ports) == 2:
                return ports

            if port not in used_ports:
                ports.append(port)

        raise DaemonException('No free port')

    def _get_pgdocker_port(self):
        used_ports = []
        for stand in self.stands.values():
            if stand.db_type == 'pgdocker':
                used_ports.append(stand.db_port)

        for port in range(self.pgdocker_start_port,
                          self.pgdocker_start_port + self.pgdocker_ports):
            if port not in used_ports:
                return port

        raise DaemonException('No free port')

    def free_resources(self) -> bool:
        """
        Можно ли запустить еще один стенд
        """
        if len(self.cli.containers(all=False)) >= self.max_active_stands:
            log.info('No resources')
            return False

        return True

    def get_stands(self, full_info=False, active_only=False, task_only=False, error_only=False) -> dict:
        """
        :param error_only: показывать только стенды с ошибками
        :param full_info: показывать расширенную информацию
        :param active_only: показывать только активные стенды
        :param task_only: показывать только стенды имеющие активные задачи
        :return: список стендов
        """
        log.debug('Get stands. full_info %s, active_only %s, task_only: %s', full_info, active_only, task_only)
        result = {}
        for stand in self.stands.values():
            name = stand.name

            info = {'url': self.get_url(name),
                    'version': stand.version,
                    'active_task': stand.active_task,
                    'web_interface_error': stand.web_interface_error,
                    }

            if full_info:
                info['description'] = stand.description
                info['db_type'] = stand.db_type
                info['db_addr'] = stand.db_addr
                info['db_port'] = stand.db_port
                info['db_name'] = stand.db_name
                info['jenkins_project'] = stand.jenkins_project
                info['jenkins_version'] = stand.jenkins_version
                info['debug_port'] = stand.ports[1]
                info['validate_entity_code'] = stand.validate_entity_code
                info['uni_schema'] = stand.uni_schema
                info['catalina_opt'] = stand.catalina_opt

            if task_only and (not stand.active_task or stand.active_task['status'] == task.ERROR):
                continue

            if error_only and not stand.web_interface_error and \
                    (not stand.active_task or stand.active_task['status'] != task.ERROR):
                continue

            if not stand.container_id:
                if active_only:
                    continue
                else:
                    info['status'] = 'uncreated'
                    result[name] = info
                    continue

            inspect_info = self.cli.inspect_container(stand.container_id)

            if stand.is_running():
                info['status'] = inspect_info['State']['Status']
                if full_info:
                    info['pid'] = inspect_info['State']['Pid']
            else:
                if active_only:
                    continue
                # 143 application was terminated due to a SIGTERM command
                # 130 Script terminated by Control-C
                # 1 error
                if inspect_info['State']['ExitCode'] == 1:
                    info['status'] = 'down'
                elif inspect_info['State']['ExitCode'] in (0, 130, 143):
                    info['status'] = 'stopped'
                else:
                    log.error(stand.name)
                    log.error(inspect_info['State'])
                    info['status'] = 'incorrect, state: {0} exit code: {1}'.format(inspect_info['State']['Status'],
                                                                                   inspect_info['State']['ExitCode'])

            result[name] = info

        return result

    def add_new(self,
                name,
                db_type,
                jenkins_project,
                db_addr=None,
                db_port=None,
                db_name=None,
                db_user=None,
                db_pass=None,
                db_container=None,
                description=None,
                jenkins_version=None,
                validate_entity_code=True,
                do_build=False,

                existed_db=False,
                backup_file=None,
                reduce=False,
                uni_schema=None,
                ) -> task.Task:
        """
        Задача на добавление нового стенда
        :param db_container: Контейнер базы данных. только для db_type=pgdocker
        :param filesystem_backup: использовать для восстановления бэкап файловой системы, только для db_type=pgdocker, бэкап должен быть на сервере
        :param uni_schema: {'user' : 'user', 'pass': 'pass'} если надо использовать схему uni
        :param validate_entity_code:Если false то добавит строку db.validateEntityCode=false в hibernate.config
        :param reduce: Почистить блобы и журнал, сжать базу
        :param do_build: перед созданием собрать новый билд
        :param existed_db: подключиться к существующей базе данных, иначе будет ошибка при попытке создать базу данных
        :param name: имя стенда
        :param db_addr: адрес базы данных для стенда (существующей или новой)
        :param db_name: имя базы данных для стенда  (существующей или новой)
        :param db_pass: логин для подключения к базе данных
        :param db_user: пароль для подключенния к базе данных
        :param jenkins_project: проект (джоб) в дженкинсе, где собирать сборку
        :param description: описание стенда
        :param jenkins_version: версия в дженкинсе
        :param db_type: postgres или mssql
        :param db_port: порт для подключения к базе данных
        :param backup_file: восстановить базу данных из бэкапа
        :return: Задача
        """
        log.debug('Add new task ADD for stand %s', name)
        if name in self.stands.keys():
            raise DaemonException('Stand with the same name is already exists')

        if existed_db and uni_schema:
            raise DaemonException('Cannot apply uni schema in existed database. '
                                  'Use specific database name or default name')

        if uni_schema and not backup_file:
            raise DaemonException('Setup uni schema is supported for database from backup mode only')

        if reduce and not backup_file and not existed_db:
            raise DaemonException('Cannot reduce clear database')

        if db_type == 'pgdocker' and existed_db and not db_port:
            raise DaemonException('You must specify db_port fom pgdocker existed db')

        if db_container and db_type != 'pgdocker':
            raise DaemonException('You can use db_container only for pgdocker db_type')

        ports = self._get_ports()

        stand_dir = os.path.join(self.stands_dir, name)

        if not db_name:
            db_name = '{}{}'.format(self.db_prefix, name).replace('-', '_')

        if db_type == 'postgres':
            if not db_addr:
                db_addr = self.postgres_addr
            if not db_user:
                db_user = self.postgres_user
            if not db_pass:
                db_pass = self.postgres_pass
            if not db_port:
                db_port = 5432
            db_container = None
            ssh_user = None
            ssh_pass = None
            pattern = self.postgres_hibernate_conf
            backup_dir = self.postgres_backup_dir

        elif db_type == 'mssql':
            if not db_addr:
                db_addr = self.mssql_addr
            if not db_user:
                db_user = self.mssql_user
            if not db_pass:
                db_pass = self.mssql_pass
            if not db_port:
                db_port = 1433
            db_container = None
            ssh_user = None
            ssh_pass = None
            pattern = self.mssql_hibernate_conf
            backup_dir = self.mssql_backup_dir

        elif db_type == 'pgdocker':
            if not db_addr:
                db_addr = self.pgdocker_addr
            if not db_port:
                db_port = self._get_pgdocker_port()
            if not db_container:
                db_container = db_name
            db_name = 'uni'
            db_user = 'postgres'
            db_pass = 'postgres'
            ssh_user = self.pgdocker_ssh_user
            ssh_pass = self.pgdocker_ssh_pass
            pattern = self.postgres_hibernate_conf
            backup_dir = self.pgdocker_backup_dir

        else:
            raise DaemonException('Unsupported database type')

        # Контейнеры по умолчанию не умеют резолвить имена, поэтому подключаемся по ip
        db_addr = socket.gethostbyname(db_addr)
        db_port = int(db_port)

        stand_details = {'image': self.image,
                         'catalina_opt': self.catalina_opt,

                         'container_id': None,
                         'name': name,
                         'description': description,
                         'stand_dir': stand_dir,
                         'ports': ports,

                         'db_type': db_type,
                         'db_addr': db_addr,
                         'db_port': db_port,
                         'db_name': db_name,
                         'db_user': db_user,
                         'db_pass': db_pass,
                         'db_container': db_container,
                         'ssh_user': ssh_user,
                         'ssh_pass': ssh_pass,
                         'backup_dir': backup_dir,

                         'last_backup': None,
                         'validate_entity_code': validate_entity_code,
                         'uni_schema': uni_schema,

                         'jenkins_project': jenkins_project,
                         'jenkins_version': jenkins_version,
                         'jenkins_url': self.jenkins_url,
                         'jenkins_user': self.jenkins_user,
                         'jenkins_pass': self.jenkins_pass,
                         'version': None,

                         'active_task': None,
                         'web_interface_error': None,
                         }

        stand = Stand(**stand_details)
        self.stands[name] = stand

        if backup_file:
            backup_path = self._backup_path(stand, file_name=backup_file)
        else:
            backup_path = None

        return task.Task(do=task.DO_ADD_NEW,
                         stand=stand,
                         config_dir=self.config_dir,
                         pattern=pattern,
                         existed_db=existed_db,
                         backup_path=backup_path,
                         reduce=reduce,
                         do_build=do_build,
                         )

    def _stand_with_validate(self, name, for_task=True):
        try:
            stand = self.stands[name]
        except KeyError:
            raise DaemonException('Stand is not exists')
        assert isinstance(stand, Stand)

        if not stand.container_id:
            raise DaemonException('Stand is not created')

        if for_task and stand.active_task and stand.active_task['status'] != task.ERROR:
            raise DaemonException('Stand has task already. Wait for task complete')

        if not for_task and stand.active_task and stand.active_task['status'] not in (task.ERROR, task.WAIT):
            raise DaemonException('Stand has active task. Wait for task complete')

        return stand

    def get_url(self, name) -> str:
        """
        :param name: название стенда
        :return: url стенда
        """
        try:
            return 'http://{0}:{1}/'.format(self.uni_docker_url, self.stands[name].ports[0])
        except KeyError:
            raise DaemonException('Stand is not exists')

    def update(self, name, change_branch=None) -> task.Task:
        """
        Задача на обновление стенда
        :param change_branch: изменить бранч из которого будет собираться сборка
        :param name: название стенда
        :return: Задача
        """
        s = self._stand_with_validate(name)
        if change_branch:
            if change_branch == 'last':
                change_branch = None

            log.info('Change jenkins version of stand %s, to version %s', name, change_branch)
            s.jenkins_version = change_branch
            s.write_json()

        log.debug('Add new task UPDATE for stand %s', name)
        return task.Task(do=task.DO_UPDATE, stand=s, do_build=True)

    @staticmethod
    def _backup_path(stand, file_name=None, prefix=None, no_join_path=False):
        """
        Возвращает название файла или абсолютный путь к файлу резервной копии c учетом настроек стенда
        :param stand: название стенда
        :param file_name: ипользовать свое имя файла
        :param prefix: ипользовать свой префикс в названии бэкапа
        :param no_join_path: вернуть только название файла, не добавлять путь
        :return:
        """
        db_type = stand.db_type
        backup_dir = stand.backup_dir

        if file_name and prefix:
            raise ValueError('You cannot use filename and prefix together')

        if not prefix:
            prefix = 'default'

        if not file_name:
            if db_type == 'postgres':
                file_name = '{}_{}.backup'.format(stand.name, prefix)
            elif db_type == 'mssql':
                file_name = '{}_{}.bak'.format(stand.name, prefix)
            elif db_type == 'pgdocker':
                file_name = '{}_{}.tar'.format(stand.name, prefix)
            else:
                raise RuntimeError('Unsupported database type')

        if not no_join_path:
            if db_type in ('postgres', 'pgdocker'):
                return os.path.join(backup_dir, file_name)
            elif db_type == 'mssql':
                return '{}\\{}'.format(backup_dir, file_name)
            else:
                raise RuntimeError('Unsupported database type')
        else:
            return file_name

    def backup_db(self, name, file=None, prefix=None) -> task.Task:
        """
        Задача на создание резервной копии базы данных с названием по умолчанию
        :param prefix: создать бэкап с использованием префикса
        :param file: файл бэкапа в директории бэкапов стенда
        :param name: название стенда
        :return: Задача
        """
        log.debug('Add new task BACKUP for stand %s', name)

        s = self._stand_with_validate(name)

        if s.web_interface_error:
            raise DaemonException('Stand has web interface error. Use specific filename for backup')

        if not s.container_id:
            raise DaemonException('Сan not to do backup for uncreated stand')

        inspect_info = self.cli.inspect_container(s.container_id)
        if inspect_info['State']['Status'] not in ('exited', 'running', 'created') and not file:
            raise DaemonException('Stand in uncertain state. Use specific filename for backup')

        if inspect_info['State']['ExitCode'] not in (0, 130, 143) and not file:
            raise DaemonException('Exit code of container is incorrect. Maybe stand is down now? Use specific filename')

        return task.Task(do=task.DO_BACKUP, stand=s,
                         backup_path=self._backup_path(stand=s, file_name=file, prefix=prefix))

    def restore_db(self, name, file=None) -> task.Task:
        """
        Задача на восстановление бд стенда из бэкапа с названием по умолчанию
        :param file: файл бэкапа в директории бэкапов стенда
        :param name: название стенда
        :return: Задача
        """
        log.debug('Add new task RESTORE for stand %s', name)
        s = self._stand_with_validate(name)
        return task.Task(do=task.DO_RESTORE, stand=s,
                         backup_path=self._backup_path(stand=s, file_name=file))

    def reduce(self, name) -> task.Task:
        log.debug('Add new task REDUCE for stand %s', name)
        s = self._stand_with_validate(name)
        return task.Task(do=task.DO_REDUCE, stand=s)

    def start(self, name, wait=True):
        """
        Запустить стенд
        :param wait: подождать запуска стенда
        :param name: название стенда
        """
        if not self.free_resources():
            raise DaemonException('Мax number of stands are running')
        self._stand_with_validate(name, for_task=False).start(wait=wait)

    def stop(self, name, wait=True):
        """
ё       Остановить стенд
        :param wait: подождать остановки стенда
        :param name: название стенда
        """
        self._stand_with_validate(name, for_task=False).stop(wait=wait)

    def catalina_out(self, name, tail=150):
        """
        Получить логи стенда
        :param name: название стенда
        :param tail: колличество строк с конца
        :return: str
        """
        log.debug('Read log file for stand %s', name)
        if name not in self.stands:
            raise DaemonException('Stand is not exists')

        if tail != 'all':
            try:
                tail = int(tail)
            except ValueError:
                tail = 150

        return self.cli.logs(self.stands[name].container_id, tail=tail)

    def daily_backup(self):
        """
        Бэкапит базы данных стендов у которых нет активных задач и которые хоть раз запускались
        Если активная задача есть - это значит что стенд может быть сломан в данный момент,
        либо можеты быть сломан после окончания таска (например, обновлением), либо уже бэкапится или ресторится

        Не бэкапит стенды, которые не запускались с момента прошлого бэкапа
        """

        for stand in self.stands.values():
            if not stand.container_id:
                log.warning('Container of stand %s is not created. Backup skipped', stand.name)
                continue

            # Перед бэкапом контейнер выключается. Если он запущен после даты последнего бэкапа, значит бэкапим заново
            inspect_info = self.cli.inspect_container(stand.container_id)

            last_start = datetime.datetime.strptime(inspect_info['State']['StartedAt'][:19], task.BACKUP_DATE_FORMAT)
            if stand.last_backup \
                    and last_start < datetime.datetime.strptime(stand.last_backup, task.BACKUP_DATE_FORMAT):
                log.info('Backup of stand %s skipped cause container was not started after last backup',
                         stand.name)
                continue

            log.info('Try to create backup for %s', stand.name)
            try:
                self.backup_db(stand.name).run()
            except DaemonException as e:
                log.warning(str(e))

    def clone_db(self,
                 name,
                 new_db_name,
                 do_backup=False):
        raise NotImplementedError

    def clone(self,
              name,
              new_name,
              new_description=None,
              new_jenkins_project=None,
              new_jenkins_version=None,
              do_build=False,
              do_backup=False):

        """
        Создать стенд с теми же характеристиками, что и исходный стенд
        :param name: Название стенда, который следует взять за основу
        :param new_name: Название нового стенда
        :param new_description: Заменить описание нового стенда
        :param new_jenkins_project: Заменить проект (джоб) в дженкинсе для нового стенда
        :param new_jenkins_version: Заменить версию в дженкинсе для нового стенда
        :param do_build: Собрать билд для нового стенда
        :param do_backup: Сделать бэкап основы перед копированием, либо взять последний бэкап по умолчанию
        :return: Лист задач
        """
        log.info('Clone %s to %s', name, new_name)
        stand = self._stand_with_validate(name)

        if not new_description:
            new_description = 'Clone of {}'.format(name)

        if not new_jenkins_project:
            new_jenkins_project = stand.jenkins_project

        if not new_jenkins_version:
            new_jenkins_version = stand.jenkins_version
        elif new_jenkins_version == 'last':
            new_jenkins_version = None

        if stand.db_type == 'pgdocker':
            db_port = None
        else:
            db_port = stand.db_port

        task_add = self.add_new(name=new_name,
                                db_type=stand.db_type,
                                jenkins_project=new_jenkins_project,
                                db_addr=stand.db_addr,
                                db_port=db_port,
                                db_name=None,
                                db_user=stand.db_user,
                                db_pass=stand.db_pass,
                                description=new_description,
                                jenkins_version=new_jenkins_version,
                                do_build=do_build,
                                existed_db=False,
                                reduce=False,
                                validate_entity_code=stand.validate_entity_code,
                                uni_schema=stand.uni_schema,
                                backup_file=self._backup_path(stand=stand, no_join_path=True),
                                )
        # Заменяем дефолтные конфиги на конфиги стенда
        task_add.task_params['config_dir'] = os.path.join(stand.stand_dir, 'config')

        # Делаем резервную копию если явно сказано сделать ее, либо если нет ни одного бэкапа
        # TODO если существует только бэкап в недефолтным именем, то сломается если do_backup=false
        if do_backup or not stand.last_backup:
            try:
                task_list = [self.backup_db(name), task_add]
            except DaemonException as e:
                del self.stands[new_name]
                raise e
        else:
            task_list = [task_add]

        return task_list
