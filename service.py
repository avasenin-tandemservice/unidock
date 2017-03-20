#!/usr/bin/python3

import argparse
import json
import logging
import logging.config
import os
import shutil

from daemon import task
from daemon.config import DaemonConfig
from daemon.exceptions import DaemonException
from daemon.stand_manager import StandManager

log = logging.getLogger('service')


def _init_args():
    parser = argparse.ArgumentParser(prog='Обновление конфигурации стендов')
    parser.add_argument('--stand-info', action='store_true',
                        help='Модифицировать stand_info.json (правило должно быть добавлено в код)')
    parser.add_argument('--containers', action='store_true',
                        help='Пересоздать все контейнеры с параметрами из конфиг файла')
    parser.add_argument('--configs', action='store_true',
                        help='Заменить конфиг файлы всех стендов на файлы из config_dir. '
                             'hibernate.properties не меняется')
    parser.add_argument('--stands-from-json', type=str,
                        help='Добавить новые стенды из json файла. Если стенд уже существует он будет пропущен. '
                             'Должны существовать файлы бэкапа и сборки в дженкинсе с этим именем')
    parser.add_argument('--backup-with-prefix', type=str,
                        help='Сделать резервную копию всех стендов tm (включая стенды с ошибками). '
                             'Указать метку бэкапа. Формат файлов баэкапа <название стенда>_<метка>')
    parser.add_argument('--daily-backup', action='store_true',
                        help='Сделать резервную копию всех исправных стендов, '
                             'которые были запущены хотя бы раз с момента поселеднего бэкапа')
    parser.add_argument('--update-all', action='store_true',
                        help='Обновить все стенды до последней версии')

    args = parser.parse_args()
    return args


def main():
    args = _init_args()

    conf = DaemonConfig().load_default()
    logging.config.dictConfig(conf.default_logging())

    if args.stand_info:
        log.info('Modify stand info')
        stands_dir = os.path.join(conf.work_dir, 'stands')

        stand_info_all = {}

        for stand_dir in os.listdir(stands_dir):
            stand_info_path = os.path.join(stands_dir, stand_dir, 'stand_info.json')
            if os.path.isfile(stand_info_path):
                with open(stand_info_path, 'rt') as f:
                    stand_info_all[stand_info_path] = json.loads(f.read())

        for stand_info_path, stand_info in stand_info_all.items():
            # Добавить правило для исправления stand_info сюда
            if 'ports' not in stand_info:
                stand_info['ports'] = [stand_info['port'], stand_info['port'] + 10]
                del stand_info['port']
                log.info('Исправлены порты в %s', stand_info_path)

            if 'validate_entity_code' not in stand_info:
                stand_info['validate_entity_code'] = True
                log.info('Добавлено validate_entity_code=True в %s', stand_info_path)

            if 'uni_schema' not in stand_info:
                stand_info['uni_schema'] = None
                log.info('Добавлено uni_schema=None в %s', stand_info_path)

            if 'last_backup' not in stand_info:
                stand_info['last_backup'] = None
                log.info('Добавлено last_backup=None в %s', stand_info_path)

            if 'db_container' not in stand_info:
                stand_info['db_container'] = None
                stand_info['ssh_user'] = None
                stand_info['ssh_pass'] = None
                log.info('Добавлены db_container ssh_user ssh_pass  в %s', stand_info_path)

            if 'web_interface_error' not in stand_info:
                stand_info['web_interface_error'] = None
                log.info('Добавлен web_interface_error в %s', stand_info_path)

            if 'backup_dir' not in stand_info:
                if stand_info['db_type'] == 'postgres':
                    backup_dir = conf.postgres_backup_dir
                elif stand_info['db_type'] == 'mssql':
                    backup_dir = conf.mssql_backup_dir
                elif stand_info['db_type'] == 'pgdocker':
                    backup_dir = conf.pgdocker_backup_dir
                else:
                    raise RuntimeError
                stand_info['backup_dir'] = backup_dir
                log.info('Добавлен backup_dir=%s в %s', backup_dir, stand_info_path)

            with open(stand_info_path, 'wt') as f:
                json.dump(stand_info, f)

    sm = StandManager(conf)

    if args.containers:
        log.info('Recreate containers')
        for stand in sm.stands.values():
            stand.image = conf.image
            stand.catalina_opt = conf.catalina_opt
            stand.write_json()

        for name, stand in sm.stands.items():
            try:
                stand.remove()
            except DaemonException:
                pass
            stand.create_container()

    if args.configs:
        log.info('Update config files of stands')
        for stand in sm.stands.values():
            stand_config_dir = os.path.join(stand.stand_dir, 'config')
            default_config_dir = os.path.join(conf.config_dir, 'config')
            src_files = os.listdir(default_config_dir)
            for file_name in src_files:
                full_file_name = os.path.join(default_config_dir, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, stand_config_dir)

    if args.stands_from_json:
        log.info('Create stands from json')
        with open(args.stands_from_json, 'rt') as f:
            stand_list = json.loads(f.read())

        for stand in stand_list:
            if stand['postgres']:
                backup_file = '{}.backup'.format(stand['name'])
                db_type = 'postgres'
            else:
                backup_file = '{}.bak'.format(stand['name'])
                db_type = 'mssql'

            jenkins_trunk = 'uni{}_trunkTest'.format(stand['name'])
            jenkins_branch = 'uni{}_branchTest'.format(stand['name'])
            description_trunk = '{} последний транк'.format(stand['description'])
            description_branch = '{} последний бранч'.format(stand['description'])

            trunk_name = 'tm_{}_t'.format(stand['name'])
            branch_name = 'tm_{}_b'.format(stand['name'])

            if 'validate_entity_code' in stand and stand['validate_entity_code'] == False:
                validate_entity_code = False
            else:
                validate_entity_code = True

            if 'uni_schema' in stand:
                uni_schema = stand['uni_schema']
            else:
                uni_schema = None

            if 'jenkins_version' in stand:
                jenkins_version = stand['jenkins_version']
            else:
                jenkins_version = None

            if branch_name not in sm.stands:
                t = sm.add_new(name=branch_name,
                               db_type=db_type,
                               jenkins_project=jenkins_branch,
                               jenkins_version=jenkins_version,
                               description=description_branch,
                               do_build=True,
                               validate_entity_code=validate_entity_code,
                               reduce=True,
                               uni_schema=uni_schema,
                               backup_file=backup_file)
                t.run(no_exceptions=True)

                if (t.status == task.ERROR or t.stand.web_interface_error) and trunk_name not in sm.stands:
                    log.error('Something wrong while create branch stand. Trunk stand creation canceled')
                    continue

                t.stand.stop()

                if jenkins_version and trunk_name not in sm.stands:
                    log.warning('Creating trunk is skipped cause version of trunk is undefined')
                    continue

            if trunk_name not in sm.stands:
                for t in sm.clone(name=branch_name,
                                  new_name=trunk_name,
                                  new_description=description_trunk,
                                  new_jenkins_project=jenkins_trunk,
                                  new_jenkins_version='last',
                                  do_build=True,
                                  do_backup=True):
                    t.run(no_exceptions=True)

                    t.stand.stop()

    if args.backup_with_prefix:
        prefix = args.backup_with_prefix
        log.info('Backup all stands with prefix %s', prefix)
        for stand in sm.stands.values():
            sm.backup_db(stand.name, prefix=prefix).run()

    if args.daily_backup:
        log.info('Daily backup')
        sm.daily_backup()

    if args.update_all:
        log.warning('Update all stands of last trunk and branch')
        for stand in sm.stands.values():
            if not stand.jenkins_version:
                sm.update(stand.name).run()
                sm.stop(stand.name)


if __name__ == '__main__':
    main()
