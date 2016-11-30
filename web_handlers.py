import logging
import os
from concurrent.futures import ThreadPoolExecutor

from tornado import gen
from tornado.web import RequestHandler

from daemon.exceptions import DaemonException
from daemon.stand_manager import StandManager

log = logging.getLogger(__name__)


class CommonHandler(RequestHandler):
    def _get_stand_manager(self):
        sm = self.application.sm
        assert isinstance(sm, StandManager)
        return sm

    def _get_fast_task_tpe(self):
        tpe = self.application.fast_task_tpe
        assert isinstance(tpe, ThreadPoolExecutor)
        return tpe

    def _get_long_task_tpe(self):
        tpe = self.application.long_task_tpe
        assert isinstance(tpe, ThreadPoolExecutor)
        return tpe


class StandHandler(CommonHandler):
    @gen.coroutine
    def get(self, name, action):
        try:
            if action == '':
                self.redirect(
                        self._get_stand_manager().get_url(name))
                return

            if action == 'start':
                duration = self.get_argument('duration', 480)
                try:
                    duration = int(duration)
                except ValueError:
                    raise DaemonException('Incorrect duration')

                sm = self._get_stand_manager()

                yield self._get_fast_task_tpe().submit(
                        sm.start, name, wait=False)

                # Выключение стенда по таймауту

                # Tornado ``Futures`` do not support cancellation at current version
                # if name in sm.stands_futures:
                #     sm.stands_futures[name].cancel()
                # Торнадовская футура передаст self в callback
                def stop_callback(f):
                    if f in sm.stands_futures.values():
                        log.info('Stop %s by timeout', name)
                        sm.stop(name)
                    else:
                        log.debug('Stand %s stop event was cancelled earlier', name)

                future = gen.sleep(duration * 60)
                future.add_done_callback(stop_callback)
                sm.stands_futures[name] = future

                self.finish('Done')
                return

            if action == 'stop':
                yield self._get_fast_task_tpe().submit(
                        self._get_stand_manager().stop, name, wait=False)
                self.finish('Done')
                return

            if action == 'update':
                task = self._get_stand_manager().update(name,
                                                        change_branch=self.get_argument('change_branch', None),
                                                        )
                self._get_long_task_tpe().submit(task.run)
                self.finish('Task added')
                return

            if action == 'backup':
                task = self._get_stand_manager().backup_db(name,
                                                           file=self.get_argument('file', None),
                                                           )
                self._get_long_task_tpe().submit(task.run)
                self.finish('Task added')
                return

            if action == 'restore':
                task = self._get_stand_manager().restore_db(name,
                                                            file=self.get_argument('file', None),
                                                            )
                self._get_long_task_tpe().submit(task.run)
                self.finish('Task added')
                return

            if action == 'log':
                tail = self.get_argument('tail', 150)
                log_text = yield self._get_fast_task_tpe().submit(
                        self._get_stand_manager().catalina_out, name, tail)
                self.finish(log_text)
                return

            if action == 'clone':
                task_l = self._get_stand_manager().clone(
                        name=name.lower(),
                        new_name=self.get_argument('new_name'),
                        new_description=self.get_argument('new_description', None),
                        new_jenkins_project=self.get_argument('new_jenkins_project', None),
                        new_jenkins_version=self.get_argument('change_branch', None),
                        do_build=self.get_argument('do_build', False),
                        do_backup=self.get_argument('do_backup', False),
                )
                for task in task_l:
                    self._get_long_task_tpe().submit(task.run)
                self.finish('Tasks added')
                return

            self.finish('Incorrect action, use: start, stop, update, log, backup, restore, clone')
            return

        except DaemonException as e:
            log.info(e)
            self.finish(str(e))

    def post(self, name, action):
        try:
            if action == 'add':
                if self.get_body_argument('no_validate', None):
                    validate_entity_code = False
                else:
                    validate_entity_code = True

                db_port = self.get_body_argument('db_port', None)
                if db_port:
                    try:
                        db_port = int(db_port)
                    except ValueError:
                        raise DaemonException('Db port should be a number')

                task = self._get_stand_manager().add_new(
                        name=name.lower(),
                        db_type=self.get_body_argument('db_type'),
                        jenkins_project=self.get_body_argument('jenkins_project'),
                        db_addr=self.get_body_argument('db_addr', None),
                        db_port=db_port,
                        db_name=self.get_body_argument('db_name', None),
                        db_user=self.get_body_argument('db_user', None),
                        db_pass=self.get_body_argument('db_pass', None),
                        description=self.get_body_argument('description', None),
                        jenkins_version=self.get_body_argument('branch', None),
                        validate_entity_code=validate_entity_code,
                        do_build=self.get_body_argument('do_build', False),
                        existed_db=self.get_body_argument('existed_db', False),
                        backup_file=self.get_body_argument('backup_file', None),
                        reduce=self.get_body_argument('reduce', False),
                        uni_schema=self.get_body_argument('uni_schema', None),
                )
                self._get_long_task_tpe().submit(task.run)
                self.finish('Task added')
                return

            if action == 'reduce':
                task = self._get_stand_manager().reduce(name)
                self._get_long_task_tpe().submit(task.run)
                self.finish('Task added')
                return

            self.finish('Incorrect action, use: add, reduce')
            return

        except DaemonException as e:
            log.info(e)
            self.finish(str(e))


class ListHandler(CommonHandler):
    @gen.coroutine
    def get(self):
        try:
            full_info = self.get_argument('full', False)
            active_only = self.get_argument('active', False)
            task_only = self.get_argument('task', False)
            error_only = self.get_argument('error', False)
            info = yield self._get_fast_task_tpe().submit(self._get_stand_manager().get_stands,
                                                          full_info=full_info,
                                                          active_only=active_only,
                                                          task_only=task_only,
                                                          error_only=error_only)
            self.finish(info)
        except DaemonException as e:
            log.info(e)
            self.finish(str(e))


class HelpHandler(CommonHandler):
    def get(self):
        with open(os.path.join(os.path.dirname(__file__), 'web_handlers_help.html')) as f:
            help_text = f.read()

        self.finish(help_text.format(addr=self._get_stand_manager().uni_docker_url,
                                     port=self._get_stand_manager().uni_docker_port))
