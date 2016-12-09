import logging
import os
import pymssql
import socket
import subprocess
import time
from threading import Timer

import magic
from docker import Client

from daemon.config import DaemonConfig
from daemon.exceptions import DaemonException

log = logging.getLogger(__name__)


class StandDb(object):
    def __init__(self, addr, name, user, password, port=None, config=None):
        self.addr = addr
        self.port = port
        self.name = name
        self.user = user
        self.password = password

        if not config:
            config = DaemonConfig().load_default()
        self.backup_timeout = config.backup_timeout
        self.restore_timeout = config.restore_timeout
        self.quick_operation_timeout = 120
        self.middle_operation_timeout = 1200

    def create(self):
        raise NotImplementedError

    def restore(self, backup_path):
        raise NotImplementedError

    def backup(self, backup_path):
        raise NotImplementedError

    def reduce(self):
        """
        Удалить из базы бОльшую часть блобов, почистить все журналы, сжать базу
        """
        raise NotImplementedError

    def set_1_1(self):
        raise NotImplementedError

    def customer_patch(self):
        """
        Костыль. Ищет в названии базы имя клиента и делает запросы помогающие нам запуститься на этой базе
        """
        raise NotImplementedError


class StandMssqlDb(StandDb):
    def __init__(self, addr, name, user, password, port=None, config=None):
        super(StandMssqlDb, self).__init__(addr, name, user, password, port, config)
        if not config:
            config = DaemonConfig().load_default()
        self.db_files_dir = config.mssql_db_dir

    def _run_sql(self, sql, timeout, connect_to_current_db=True, non_query=True, ignore_errors=False):
        log.debug('Run sql. Server %s, timeout %s, query %s', self.addr, timeout, sql)

        if not self.port:
            self.port = 1433
        kw = {'server': self.addr,
              'user': self.user,
              'password': self.password,
              'port': self.port,
              'timeout': timeout}
        if connect_to_current_db:
            kw['database'] = self.name

        with pymssql.connect(**kw) as conn:
            conn.autocommit(True)
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except pymssql.Error as e:
                if ignore_errors:
                    log.warning(str(e))
                else:
                    raise e
            if not non_query:
                l = cursor.fetchall()
                log.debug('Result: %s', l)
                return l
            else:
                return cursor.rowcount

    def create(self):
        log.info('Create database %s on server %s', self.name, self.addr)
        sql = 'CREATE DATABASE {name} ON (NAME = {name}_Data, FILENAME = \'{path}\{name}.mdf\') ' \
              'LOG ON (NAME = {name}_Log,  FILENAME = \'{path}\{name}.ldf\');'.format(name=self.name,
                                                                                      path=self.db_files_dir)
        self._run_sql(sql, timeout=self.quick_operation_timeout, connect_to_current_db=False)
        self._run_sql('ALTER DATABASE {} SET READ_COMMITTED_SNAPSHOT ON;'.format(self.name),
                      timeout=self.quick_operation_timeout)
        self._run_sql('ALTER DATABASE {} SET ALLOW_SNAPSHOT_ISOLATION ON;'.format(self.name),
                      timeout=self.quick_operation_timeout)

    def backup(self, backup_path):
        log.info('Backup database %s on server %s', self.name, self.addr)
        sql = 'BACKUP DATABASE {} TO DISK = \'{}\' WITH INIT'.format(self.name,
                                                                     backup_path)
        self._run_sql(sql, timeout=self.backup_timeout)

    def restore(self, backup_path):
        log.info('Restore database %s on server %s', self.name, self.addr)
        # Сначала узнаем какие файлы содержит бэкапю Возвращает таблицу
        sql = 'RESTORE FILELISTONLY FROM DISK = \'{}\''.format(backup_path)
        file_list = self._run_sql(sql, timeout=self.quick_operation_timeout, non_query=False,
                                  connect_to_current_db=False)

        # Формируем скл запрос, который содержит правильные пути до файлов базы данных
        sql_part = []

        for elem in file_list:
            new_filename = '{}\{}_{}.{}'.format(self.db_files_dir,
                                                self.name,
                                                elem[6],  # индекс файла
                                                ('LDF' if elem[2] == 'L' else 'MDF'))  # L или D. Лог или данные

            sql_part.append('MOVE \'{}\' TO \'{}\''.format(elem[0], new_filename))  # логическое имя
        sql = 'RESTORE DATABASE {} FROM DISK = \'{}\' WITH RECOVERY, REPLACE, {};' \
            .format(self.name,
                    backup_path,
                    ', '.join(sql_part))
        self._run_sql(sql, self.restore_timeout, connect_to_current_db=False)

        # Изменить логические имена на новое имя базы данных. Если это файл лога, то добавить log, иначе номер файла
        # Нужно для шринка и очистки и чтобы не было одинаковых логических имен, что потенциально может давать глюки
        for elem in file_list:
            current_name = elem[0].lower()
            good_name = '{}_{}'.format(self.name, ('log' if elem[2] == 'L' else elem[6]))
            if current_name != good_name:
                self._run_sql(
                        'ALTER DATABASE {} MODIFY FILE (NAME = \'{}\', NEWNAME = \'{}\')'.format(self.name,
                                                                                                 current_name,
                                                                                                 good_name),
                        timeout=self.quick_operation_timeout)

    def customer_patch(self):
        if self.name.find('fefu') != -1:
            log.info('Выполнение sql специфичных для базы ДВФУ (fefu)')
            # Чистим 60+ ГБ
            self._run_sql('use {}; '
                          'truncate table FEFU_RATING_PKG_STUDENT_ROW; '
                          'alter table FEFU_RATING_PKG_STUDENT_ROW drop constraint fk_ratingpackage_96afe8ba; '
                          'truncate table FEFU_SENDING_RATING_PKG; '
                          'ALTER TABLE FEFU_RATING_PKG_STUDENT_ROW ADD CONSTRAINT fk_ratingpackage_96afe8ba '
                          'FOREIGN KEY (RATINGPACKAGE_ID) REFERENCES FEFU_SENDING_RATING_PKG(ID);'.format(self.name),
                          timeout=self.quick_operation_timeout, ignore_errors=True)
            # И еще 3+ ГБ Nsi до кучи
            self._run_sql('use {}; truncate table FEFUNSILOGROW_T'.format(self.name),
                          timeout=self.quick_operation_timeout, ignore_errors=True)

    def map_user_schema(self, user, schema):
        log.info('Map user %s to schema %s in database %s on server %s', user, schema, self.name, self.addr)
        sql = 'use {db}; CREATE USER {user} FOR LOGIN {user}; ' \
              'ALTER USER {user} WITH DEFAULT_SCHEMA={schema}; ' \
              'exec sp_addrolemember \'db_owner\', \'{user}\';' \
            .format(db=self.name, user=user, schema=schema)
        self._run_sql(sql, timeout=self.quick_operation_timeout, ignore_errors=True)

    def reduce(self):
        log.info('Reduce database %s on server %s', self.name, self.addr)
        # Отключаем лог транзакций
        self._run_sql('ALTER DATABASE {} SET RECOVERY SIMPLE; '.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

        # Сразу уменьшим логи транзакций чтобы создать больше места
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Чистим логи uni. Констреинт будет создан автоматически платформой при запуске
        self._run_sql('use {}; '
                      'truncate table logeventproperty_t; '
                      'alter table logeventproperty_t drop constraint fk_event_logeventproperty; '
                      'truncate table logevent_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

        # Чистим логи nsi если они есть
        self._run_sql('use {}; truncate table nsientitylog_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)

        # Удаляем содержимое таблиц, хранящих печатные формы различных документов, если они есть
        self._run_sql('use {}; truncate table STUDENTEXTRACTTEXTRELATION_T;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table StudentOrderTextRelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table stdntothrordrtxtrltn_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table employeeordertextrelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table employeeextracttextrelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table session_doc_printform_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table session_att_bull_printform_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)

        # Удаляем файлы, хранящиеся в базе данных. Mssql пылесос блобов работает в фоне,
        # поэтому удаляем серией маленьких транзакций по 500 файлов

        def timeout_err():
            log.error('Timeout while removing database files. Stop operation')

        t = Timer(self.restore_timeout, timeout_err)
        t.start()
        while not t.finished.is_set():
            if self._run_sql(
                    'use {}; update top(1000) databasefile_t set content_p = null where content_p is not null and '
                    '(filename_p not in (\'platform-variables.less\', \'platform.css\', \'shared.css\') '
                    'or filename_p is null);'.format(self.name),
                    timeout=self.middle_operation_timeout, ignore_errors=True, non_query=True) == 0:
                t.cancel()
                break

        # Ждем пока пылесос подчистит оставшееся, если мы начнем шринкать до этого момента,
        # то пылесосить дальше он будет после шринка, что приведет к тому что база будет ужата не полностью
        size_before = 999999999
        while 1:
            table_row = self._run_sql('sp_spaceused DATABASEFILE_T;'.format(self.name),
                                      ignore_errors=False, non_query=False, timeout=self.quick_operation_timeout)[0]
            # третья строка содержит размер данных таблицы,в виде строки, убираю ' KB' чтобы получить число
            size_at_moment = int(table_row[3][:-3])
            if size_at_moment == size_before:
                break
            else:
                size_before = size_at_moment
                time.sleep(60)

        # Еще раз удаляем лог транзакций после операции update
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Уменьшаем базу, освобождаем место на диске. Оставляем 5% свободного места
        self._run_sql('DBCC SHRINKDATABASE ({}, 5);'.format(self.name),
                      timeout=self.restore_timeout, ignore_errors=False)

        # Еще раз удаляем лог транзакций последний операций
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Включаем полноценный лог транзакций
        self._run_sql('ALTER DATABASE {} SET RECOVERY FULL; '.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

    def set_1_1(self):
        log.info('Set user and password 1:1 in database %s on server %s', self.name, self.addr)
        sql = 'use {}; ' \
              'UPDATE principal_t SET LOGIN_P=\'1\', passwordhash_p=\'c4ca4238a0b923820dcc509a6f75849b\', passwordsalt_p=null ' \
              'where ' \
              '(EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\')  and LOGIN_P=\'1\') or ' \
              '(not EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and id=(select top 1 id from PRINCIPAL_T where id in (select PRINCIPAL_ID from ADMIN_T) and ACTIVE_P=1));' \
            .format(self.name)
        self._run_sql(sql, timeout=self.quick_operation_timeout)


class StandPostgresDb(StandDb):
    def __init__(self, addr, name, user, password, port=None, config=None):
        super(StandPostgresDb, self).__init__(addr, name, user, password, port, config)
        if not config:
            config = DaemonConfig().load_default()
        self.ignore_restore_errors = config.postgres_ignore_restore_errors

    def _run_console_command(self, args, timeout, ignore_error=False, stdin=None):
        common = [
            '--host', self.addr,
            '--username', self.user,
        ]
        if self.port:
            common.extend(['--port', str(self.port)])
        common.reverse()
        for elem in common:
            args.insert(1, elem)

        log.debug('Run process with command: %s', ' '.join(args))
        os.putenv('PGPASSWORD', self.password)
        process = subprocess.Popen(args=args,
                                   stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=stdin)
        out, err = process.communicate(timeout=timeout)
        if process.returncode != 0:
            log.warning(' '.join(args))
            # Чтобы ошибки восстановления не засирали лог вывводим первые 1000 символов
            error_text = out.decode() + err.decode()
            if len(error_text) < 1000:
                log.warning(error_text)
            else:
                log.warning('{} \n and another {} symbols'.format(error_text[-1000:], len(error_text) - 1000))
            if not ignore_error:
                raise DaemonException('Console command for postgresql failed. See log for details')

    def create(self):
        log.info('Create database %s on server %s', self.name, self.addr)
        args = ['psql',
                '--command', 'CREATE DATABASE {0}'.format(self.name),
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout)

    def drop(self):
        log.info('Drop database %s on server %s', self.name, self.addr)
        self._run_console_command(['psql',
                                   '--command', 'DROP DATABASE {0}'.format(self.name),
                                   ], timeout=self.quick_operation_timeout)

    def backup(self, backup_path):
        log.info('Backup database %s on server %s', self.name, self.addr)
        args = ['pg_dump',
                '--dbname', self.name,
                '--format', 'c',
                '--file', backup_path,
                ]
        self._run_console_command(args, self.backup_timeout)

    def restore(self, backup_path):
        log.info('Restore database %s on server %s', self.name, self.addr)
        if not os.path.isdir(backup_path) and magic.from_file(backup_path, mime=False).find('ASCII text') != -1:
            # Чтобы наследники юзали методы родителя
            StandPostgresDb.drop(self)
            StandPostgresDb.create(self)
            log.info('Restore plain text backup to database %s on server %s', self.name, self.addr)
            with open(backup_path) as f:
                args = ['psql', '--quiet',
                        '--dbname', self.name,
                        ]
                if self.ignore_restore_errors:
                    self._run_console_command(args, self.restore_timeout, ignore_error=True, stdin=f)
                else:
                    self._run_console_command(args, self.restore_timeout, ignore_error=False, stdin=f)

        elif os.path.isdir(backup_path) \
                or magic.from_file(backup_path, mime=False).find('PostgreSQL') != -1 \
                or magic.from_file(backup_path, mime=False).find('POSIX tar archive') != -1:
            StandPostgresDb.drop(self)
            StandPostgresDb.create(self)
            log.info('Restore pg_dump backup to database %s on server %s', self.name, self.addr)
            args = ['pg_restore',
                    '--no-owner', '--no-privileges',
                    '--dbname', self.name,
                    backup_path,
                    ]

            if self.ignore_restore_errors:
                self._run_console_command(args, self.restore_timeout, ignore_error=True)
            else:
                args.insert(-1, '--exit-on-error')
                self._run_console_command(args, self.restore_timeout, ignore_error=False)
        else:
            log.error('File type of backup %s', magic.from_file(backup_path, mime=False))
            raise DaemonException('Wrong postgres backup format')

    def customer_patch(self):
        if self.name.find('pgups') != -1:
            log.info('Выполнение sql специфичных для базы ПГУПС (pgups)')
            args = ['psql',
                    '--dbname', self.name,
                    '--command', 'update app_info_s set value_p=\'unipgups-web\'',
                    ]
            self._run_console_command(args, timeout=self.quick_operation_timeout, ignore_error=True)

    def reduce(self):
        log.info('Reduce database %s on server %s', self.name, self.addr)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'truncate logevent_t cascade;',
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'truncate nsientitylog_t;',
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout, ignore_error=True)

        # Удаляем содержимое таблиц, хранящих печатные формы различных документов, если они есть
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table STUDENTEXTRACTTEXTRELATION_T;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table StudentOrderTextRelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table stdntothrordrtxtrltn_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table employeeordertextrelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table employeeextracttextrelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table session_doc_printform_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table session_att_bull_printform_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'update databasefile_t set content_p = null where content_p is not null and '
                             '(filename_p is null '
                             'or filename_p not in (\'platform-variables.less\', \'platform.css\', \'shared.css\'));',
                ]
        self._run_console_command(args, timeout=self.restore_timeout, ignore_error=True)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'vacuum full;',
                ]
        self._run_console_command(args, timeout=self.restore_timeout)

    def set_1_1(self):
        log.info('Set user and password 1:1 in database %s on server %s', self.name, self.addr)
        sql = 'UPDATE principal_t SET LOGIN_P=\'1\', passwordhash_p=\'c4ca4238a0b923820dcc509a6f75849b\', passwordsalt_p=null ' \
              'where ' \
              '(EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and LOGIN_P=\'1\') or ' \
              '(not EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and id=(select id from PRINCIPAL_T where id in (select PRINCIPAL_ID from ADMIN_T) and ACTIVE_P=true limit 1));'
        args = ['psql',
                '--dbname', self.name,
                '--command', sql,
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout)


class StandDockerPostgres(StandPostgresDb):
    def __init__(self, addr, container_name, ssh_user, ssh_password, port, config=None):
        # База данных в контейнере всегда uni, юзер и пароль postgres, нет смысла менять
        super(StandDockerPostgres, self).__init__(addr, 'uni', 'postgres', 'postgres', port, config)

        self.container_name = container_name
        self.ssh_user = ssh_user
        self.ssh_pass = ssh_password
        if not config:
            config = DaemonConfig().load_default()
        if config.pgdocker_use_ssh:
            raise NotImplementedError
        else:
            self.docker = Client(base_url='unix://var/run/docker.sock')

    def _create_container(self):
        self.docker.create_container(image='postgres:9.4',
                                     name=self.container_name,
                                     detach=True,
                                     ports=[5432],
                                     host_config=self.docker.create_host_config(
                                             port_bindings={5432: self.port}),
                                     environment={'POSTGRES_PASSWORD': 'postgres',
                                                  'TZ': 'Asia/Yekaterinburg'},
                                     )

    def start(self):
        self.docker.start(self.container_name)
        # Ждем запуска постргреса
        for i in range(0, 60):
            try:
                time.sleep(1)
                sock = socket.create_connection((self.addr, self.port), timeout=60)
                sock.close()
                return
            except ConnectionRefusedError:
                pass
        raise TimeoutError('Pgdocker was not started')

    def create(self):
        log.info('Create container with postgres_db. Name %s, port %s', self.container_name, self.port)
        self._create_container()
        self.start()
        # Теперь создадим дефолтную базу в новом контейнере
        # Сразу после поднятия psql: FATAL:  the database system is starting up
        for c in range(0, 10):
            try:
                time.sleep(2)
                super(StandDockerPostgres, self).create()
                break
            except DaemonException as e:
                if c == 9:
                    raise e

    def backup(self, backup_path):
        log.info('Backup database container %s on server %s', self.container_name, self.addr)
        # https://www.postgresql.org/docs/9.4/static/backup-file.html
        # The database server must be shut down in order to get a usable backup
        self.docker.stop(self.container_name, timeout=60)
        self.docker.wait(self.container_name)
        # Я использую subprocess, чтобы бэкапы не гонялись по сети.
        command = 'docker cp {}:/var/lib/postgresql/data/. - > {}'.format(self.container_name, backup_path)
        log.debug('Run command %s', command)
        try:
            subprocess.check_call(command, shell=True, timeout=self.backup_timeout)
        finally:
            self.start()

    def restore(self, backup_path):
        # Если это tar архив, то пробуем развернуть его как filesystem backup в остальных случаях пытаемся
        # обработать его как стандартный архив постгреса
        command = ['file', backup_path]
        if subprocess.check_output(command, timeout=self.quick_operation_timeout).decode(). \
                find('POSIX tar archive') != -1:
            log.info('Restore filesystem backup for container %s on server %s', self.container_name, self.addr)
            # Сначала сделуюет почистить текущие файлы базы данных, для этого удаляем контейнер вместе с томом бд
            # Контейнер не остановлен, используем флаг force
            self.docker.remove_container(self.container_name, v=True, force=True)
            self._create_container()
            command = 'docker cp - {}:/var/lib/postgresql/data < {}'.format(self.container_name, backup_path)
            subprocess.check_call(command, timeout=self.restore_timeout, shell=True)
            self.start()
        else:
            super(StandDockerPostgres, self).restore(backup_path)
