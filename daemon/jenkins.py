import logging
import os
import shutil
import time
import zipfile

import jenkinsapi
import pytz

from daemon.exceptions import DaemonException

log = logging.getLogger(__name__)


class Jenkins:
    def __init__(self, url, user, password):
        # https://jenkinsapi.readthedocs.io/en/latest/build.html
        self.server = jenkinsapi.jenkins.Jenkins(url,
                                                 username=user,
                                                 password=password)

    def version(self):
        """
        :return: Версия Jenkins
        """
        return self.server.version

    def build_project(self, project, version=None):
        """
        Запускает job и ждет окончания
        :param project: Название job в Jenkins
        :param version: Subversion entry идентично селекту в Jenkins
        :return:Номер сборки
        """
        log.debug('call build project %s and version %s', project, version)
        s = self.server
        job = s[project]

        if version:
            params = {'Version': version}
        else:
            params = None

        build_number = job.get_next_build_number()
        log.info('start build project %s with params %s, build %s', project, params, build_number)
        s.build_job(project, params)

        # jenkinsapi не считает билд существующим пока не начнется его фактическая сборка, поэтому приходится писать так
        elapsed_time = 0
        while 1:
            if elapsed_time > 1200:
                raise DaemonException('Timeout while jenkins building')
            try:
                if not job.get_build(build_number).is_running():
                    break
            except (KeyError, jenkinsapi.custom_exceptions.NotFound):
                pass
            log.debug('wait build')
            elapsed_time += 15
            time.sleep(15)

        if not job.get_build(build_number).is_good():
            raise DaemonException('Last build is incorrect')

        return build_number

    def get_build(self, project, dir_for_files, build_number=None):
        """
        Скачивает и распаковывает war файл
        :param project: Название job в Jenkins
        :param dir_for_files: Директория для распаковки war
        :param build_number: Номер сборки
        """
        log.info('Get build. Project: %s, directory: %s, build: %s', project, dir_for_files, build_number)
        s = self.server
        job = s[project]

        if build_number:
            build = job.get_build(build_number)
        else:
            build = job.get_last_build()
            build_number = build.get_number()

        log.debug('%s build status %s', build.get_number(), build.get_status())

        if not build.is_good():
            raise DaemonException('Last build of project %s is not SUCCESS' % project)

        war_file = os.path.join(dir_for_files, 'last_build.war')

        if os.path.exists(war_file):
            log.debug('del previous war')
            os.remove(war_file)

        log.debug('start loading build artifact for project %s and build number %s', project, build_number)
        for war in build.get_artifacts():
            assert isinstance(war, jenkinsapi.artifact.Artifact)

            if os.path.isdir(dir_for_files):
                log.info('Remove directory %s', dir_for_files)
                shutil.rmtree(dir_for_files)

            os.mkdir(dir_for_files)

            log.debug('download %s to %s', war.filename, war_file)

            try:
                war.save(war_file, strict_validation=False)
            # костыль, не знаю почему исключение
            except jenkinsapi.custom_exceptions.ArtifactBroken:
                pass

            if not zipfile.is_zipfile(war_file):
                raise DaemonException('Cannot unpack build artifact. It is not zip file')

            log.debug('Unpack war')
            f = zipfile.ZipFile(war_file)
            f.extractall(path=dir_for_files)
            f.close()

            build_ts = build.get_timestamp()
            local_datetime_string = build_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Yekaterinburg')) \
                .strftime('%d.%m.%Y %H:%M')

            return '{0} build {1}'.format(local_datetime_string, build_number)
