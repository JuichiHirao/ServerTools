import dropbox
from datetime import datetime
import gzip
import subprocess
import docker
import os
import time


class DropboxMysqlBackup:

    def __init__(self):

        f_cred = open('token.txt')
        token = f_cred.read()[:-1]
        self.dbx = dropbox.Dropbox(token)

    def get_today_newest(self, database_name):
        print('dropbox get newest backup start {}'.format(datetime.now()))

        result_response = self.dbx.files_list_folder(path='')
        str_date = datetime.now().strftime('%Y%m%d')
        target_prefix = 'mysql_{}'.format(database_name)
        target_filename = ''
        today_file_list = []
        for entry in result_response.entries:
            # if str_date in entry.path_display and 'mysql_av' in entry.path_display:
            if str_date in entry.path_display and target_prefix in entry.path_display:
                today_file_list.append(entry.path_display)
                # target_filename = entry.path_display
                # print(entry.path_display)
        # print('{}'.format(result_response))

        target_gz_filename = ''
        if len(today_file_list) > 0:
            today_file_list.sort(reverse=True)
            target_gz_filename = today_file_list[0].replace('/', '')
        else:
            print('today no file {}'.format(str_date))
            return ''

        target_filename = target_gz_filename.replace('.gz', '')
        target_gz_pathname = os.path.join('.\\data', target_gz_filename)
        target_pathname = os.path.join('.\\data', target_filename)
        if os.path.isfile(target_pathname):
            if os.path.isfile(target_gz_pathname):
                os.remove(target_gz_pathname)
            print('exist file {}'.format(target_pathname))
            return target_pathname

        with open(target_gz_pathname, "wb") as f:
            metadata, res = self.dbx.files_download(path='/' + target_gz_filename)
            f.write(res.content)

        with gzip.open(target_gz_pathname, mode='rt', encoding='utf-8') as f:
            file_line_list = f.readlines()

        replace_line_list = []
        for line in file_line_list:
            re_line = line.replace('` text', '` varchar(1000)')
            re_line = re_line.replace('ENGINE=InnoDB', 'ENGINE=MEMORY')
            replace_line_list.append(re_line)

        target_pathname = os.path.join('.\\data', target_filename)
        with open(target_pathname, "w", encoding='utf-8') as f:
            f.writelines(replace_line_list)

        os.remove(target_gz_pathname)

        return target_pathname


class DockerMysql:

    def run(self):

        self.remove_exist_mysql()

        print('docker mysql run start {}'.format(datetime.now()))
        cli = docker.DockerClient(base_url="tcp://localhost:2375")

        # https://docker-py.readthedocs.io/en/stable/containers.html
        env_list = ["MYSQL_ROOT_PASSWORD=mysql"]
        cli.containers.run(image='mysql:latest', auto_remove=True, environment=env_list
                           , ports={'3306/tcp': 43306})
        print('docker mysql run end {}'.format(datetime.now()))

    def remove_exist_container(self):
        cli = docker.DockerClient(base_url="tcp://localhost:2375")
        container_list = cli.containers.list()
        for one_container in container_list:
            container = cli.containers.get(one_container.id)
            container.stop()
            container.remove()
            print('docker remove {}'.format(container.id))

    def get_contaner_id(self):
        cli = docker.DockerClient(base_url="tcp://localhost:2375")

        container_list = cli.containers.list()
        container_id = ''
        for container in container_list:
            container_id = container.id
            print(container.id)

        return container_id

    def set_global_sie(self):
        cli = docker.DockerClient(base_url="tcp://localhost:2375")

        container_list = cli.containers.list()
        # 00_init.sqlで同じSQLを実行しているが有効にならず
        # importで「The table 'contents' is fullが発生するので、container_execで設定
        for container in container_list:
            container_id = container.id
            container.exec_run(
                cmd='mysql --user=root --password=mysql -e "SET GLOBAL tmp_table_size = 1024 * 1024 * 1024 * 2"')
            print('{} set global tmp_table_size'.format(container.id))
            container.exec_run(
                cmd='mysql --user=root --password=mysql -e "SET GLOBAL max_heap_table_size = 1024 * 1024 * 1024 * 2"')
            print('{} set global max_heap_table_size'.format(container.id))
            print(container.id)

    def execute_compose(self):

        os.chdir(".\\docker-mysql")
        cmd_docker_compose = 'docker-compose up -d'
        result_code = subprocess.run(cmd_docker_compose)
        print('docker compose result {}'.format(result_code))
        os.chdir("..")

    def dump_import(self, dump_pathname, database_name, container_id):

        # docker exec -i 2b32701f631c sh -c \'exec mysql --user=root --password=mysql av\' < .\\mysql_av_20200126-040501.dump
        # ['docker', 'exec', '-i', '018bbbe7c77e', 'sh', '-c', 'exec mysql --user=root --password=mysql av'],
        with open(dump_pathname, "rb") as rf:
            result_code = subprocess.run(
                ['docker', 'exec', '-i', str(container_id), 'sh', '-c', 'exec mysql --user=root --password=mysql ' + database_name],
                stdin=rf)
            print('import {}'.format(result_code))


if __name__ == '__main__':
    mysql_backup = DropboxMysqlBackup()
    mysqldump_pathname = mysql_backup.get_today_newest('av')
    print(mysqldump_pathname)

    docker_mysql = DockerMysql()

    docker_mysql.remove_exist_container()
    docker_mysql.execute_compose()

    print('sleeping... 10sec')
    time.sleep(10)

    container_id = docker_mysql.get_contaner_id()
    docker_mysql.set_global_sie()

    docker_mysql.dump_import(mysqldump_pathname, 'av', container_id)

