import datetime
import logging
import os

from daemon.jenkins import Jenkins
from daemon.stand import Stand

log = logging.getLogger(__name__)

WAIT = 'WAIT'
RESTORE_DB = 'RESTORE_DB'
BACKUP_DB = 'BACKUP_DB'
REDUCE = 'REDUCE'
CREATE_DIR = 'CREATE_DIR'
CREATE_DB = 'CREATE_DB'
CREATE_CONTAINER = 'CREATE_CONTAINER'
BUILD_AND_UPLOAD = 'BUILD_AND_UPLOAD'
TEST_RUN = 'TEST_RUN'
ERROR = 'ERROR'

DO_ADD_NEW = 'ADD_NEW'
DO_UPDATE = 'UPDATE'
DO_BACKUP = 'BACKUP'
DO_RESTORE = 'RESTORE'
DO_REDUCE = 'REDUCE'

BACKUP_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class Task:
    def __init__(self, do, stand, **kwargs):
        self.do = do
        assert isinstance(stand, Stand)
        self.stand = stand

        self.task_params = kwargs

        self.status = None
        self.error = None

        self.jenkins = Jenkins(self.stand.jenkins_url, self.stand.jenkins_user, self.stand.jenkins_pass)

        self.set_status(WAIT)

    def set_status(self, new_status):
        log.info('Task %s of stand %s has status %s', self.do, self.stand.name, new_status)

        if new_status is None:
            self.stand.active_task = None
        elif new_status is ERROR:
            d = {'do': self.do, 'status': new_status, 'error': self.error,
                 'task_params': self.task_params, 'while': self.status}
            self.stand.active_task = d
        else:
            self.stand.active_task = {'do': self.do, 'status': new_status, 'task_params': self.task_params}
        self.status = new_status
        self.stand.write_json()

    def write_version_file(self):
        log.debug('Write version file')
        with open(os.path.join(self.stand.stand_dir, 'config', 'version.txt'), 'wt') as f:
            f.writelines(['Unidock name: {}, Jenkins job: {} {} at {}'.format(self.stand.name,
                                                                              self.stand.jenkins_project,
                                                                              self.stand.jenkins_version or '',
                                                                              self.stand.version),
                          ])

    def _add_new(self):
        try:
            config_dir = self.task_params['config_dir']
            pattern = self.task_params['pattern']
            existed_db = self.task_params['existed_db']
            backup_path = self.task_params['backup_path']
            do_build = self.task_params['do_build']
            reduce = self.task_params['reduce']
        except KeyError:
            raise RuntimeError('Missing parameter of task')

        if not existed_db:
            self.set_status(CREATE_DB)
            self.stand.db.create()

        if backup_path:
            self.set_status(RESTORE_DB)
            self.stand.db.restore(backup_path=backup_path)

            if self.stand.db_type == 'mssql' and self.stand.uni_schema:
                self.stand.db.map_user_schema(self.stand.uni_schema['user'], 'uni')
                self.stand.db_user = self.stand.uni_schema['user']
                self.stand.db_pass = self.stand.uni_schema['pass']
                self.stand.write_json()
                self.stand.db.user = self.stand.uni_schema['user']
                self.stand.db.password = self.stand.uni_schema['pass']

            self.stand.db.customer_patch()
            self.stand.db.set_1_1()

        if reduce:
            self.set_status(REDUCE)
            self.stand.db.reduce()

        # создать структуру директорий стенда и конфиги
        self.set_status(CREATE_DIR)
        self.stand.create_dir_structure(config_dir, pattern)

        # создать докер контейнер без запуска. подключить к созданной директории
        self.set_status(CREATE_CONTAINER)
        self.stand.create_container()

        # собрать и загрузить файлы webapp
        self.set_status(BUILD_AND_UPLOAD)
        if do_build:
            build = self.jenkins.build_project(self.stand.jenkins_project, self.stand.jenkins_version)
        else:
            build = None
        self.stand.version = self.jenkins.get_build(self.stand.jenkins_project,
                                                    os.path.join(self.stand.stand_dir, 'webapp'),
                                                    build)
        self.write_version_file()

        self._test_run()

    def _test_run(self):
        self.set_status(TEST_RUN)
        self.stand.start()
        self.set_status(None)

    def _reduce(self):
        self.stand.stop(wait=True)
        self.set_status(REDUCE)
        self.stand.db.reduce()
        self.set_status(None)

    def _update(self):
        try:
            do_build = self.task_params['do_build']
        except KeyError:
            raise RuntimeError('Missing parameter of task')

        self.stand.stop(wait=True)
        self.set_status(BUILD_AND_UPLOAD)
        webapp_dir = os.path.join(self.stand.stand_dir, 'webapp')
        if do_build:
            build = self.jenkins.build_project(self.stand.jenkins_project, self.stand.jenkins_version)
        else:
            build = None
        self.stand.version = self.jenkins.get_build(self.stand.jenkins_project,
                                                    webapp_dir,
                                                    build)
        self.write_version_file()

        self._test_run()

    def _restore_db(self):
        try:
            backup_path = self.task_params['backup_path']
        except KeyError:
            raise RuntimeError('Missing parameter of task')

        self.stand.stop(wait=True)

        self.set_status(RESTORE_DB)
        self.stand.db.restore(backup_path=backup_path)
        self.set_status(None)

    def _backup_db(self):
        try:
            backup_path = self.task_params['backup_path']
        except KeyError:
            raise RuntimeError('Missing parameter of task')

        self.stand.stop(wait=True)

        self.set_status(BACKUP_DB)
        self.stand.db.backup(backup_path=backup_path)
        self.stand.last_backup = datetime.datetime.utcnow().strftime(BACKUP_DATE_FORMAT)
        self.set_status(None)

    def run(self, no_exceptions=True):
        try:
            available = self.stand.is_running()

            if self.do == DO_ADD_NEW:
                self._add_new()

            elif self.do == DO_UPDATE:
                self._update()

            elif self.do == DO_BACKUP:
                self._backup_db()

            elif self.do == DO_RESTORE:
                self._restore_db()

            elif self.do == DO_REDUCE:
                self._reduce()

            else:
                log.error('Unsupported task "do"')
                if not no_exceptions:
                    raise RuntimeError('Unsupported task "do"')

            if available:
                self.stand.start(wait=False)

        except Exception as e:
            if not no_exceptions:
                raise e

            self.error = str(e)
            self.set_status(ERROR)
            log.exception(e)
            return
