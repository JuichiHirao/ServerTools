import dropbox
from datetime import datetime
import gzip
import subprocess
import docker
import os


class DropboxMysqlBackup:

    def __init__(self):
        self.token_filename = 'token.txt'

    def get_today(self):
        print('mysql backup start {}'.format(datetime.now()))
        # args = sys.argv
        # filename = args[1]

        f_cred = open(self.token_filename)
        token = f_cred.read()[:-1]
        # print(token)
        dbx = dropbox.Dropbox(token)

        result_response = dbx.files_list_folder(path='')
        str_date = datetime.now().strftime('%Y%m%d')
        target_filename = ''
        for entry in result_response.entries:
            if str_date in entry.path_display and 'mysql_av' in entry.path_display:
                target_filename = entry.path_display
                print(entry.path_display)
        # print('{}'.format(result_response))

        with open(target_filename.replace('/', ''), "wb") as f:
            metadata, res = dbx.files_download(path=target_filename)
            f.write(res.content)

        with gzip.open(target_filename.replace('/', ''), mode='rt', encoding='utf-8') as f:
            file_line_list = f.readlines()

        replace_line_list = []
        for line in file_line_list:
            re_line = line.replace('` text', '` varchar(1000)')
            re_line = re_line.replace('ENGINE=InnoDB', 'ENGINE=MEMORY')
            replace_line_list.append(re_line)

        filename = '01_{}'.format(target_filename.replace('/', '').replace('.gz', ''))
        target_pathname = os.path.join('.\\docker-mysql\\initdb', filename)
        with open(target_pathname, "w", encoding='utf-8') as f:
            f.writelines(replace_line_list)

        return target_filename.replace('/', '').replace('.gz', '')


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

    def remove_exist_mysql(self):
        cli = docker.DockerClient(base_url="tcp://localhost:2375")
        container_list = cli.containers.list()
        for one_container in container_list:
            container = cli.containers.get(one_container.id)
            container.stop()
            # container.remove()
            print('docker remove {}'.format(container.id))

    def generate_mysql(self, mysqldump_filename):
        cli = docker.DockerClient(base_url="tcp://localhost:2375")
        data_list = cli.images.list()
        print(data_list)

        container_list = cli.containers.list()
        container_id = 0
        for container in container_list:
            container_id = container.id
            print(container.id)

        """
        container = cli.containers.get(container_id)
        container.exec_run(cmd='mysql --user=root --password=mysql -e "CREATE DATABASE av"')
        print('{} create database av'.format(container.id))
        exit_code, output = container.exec_run(cmd='mysql --user=root --password=mysql -e "CREATE USER admin@\'%\' identified by \'Mdbsys5904@\'"')
        print('{} create user admin  result{} {}'.format(container.id, exit_code, output))
        exit_code, output = container.exec_run(cmd='mysql --user=root --password=mysql -e "grant all on *.* to \'admin\'@\'%\'"')
        print('{} grant all  result {} {}'.format(container.id, exit_code, output))
        container.exec_run(cmd='mysql --user=root --password=mysql -e "SET GLOBAL tmp_table_size = 1024 * 1024 * 1024 * 2"')
        print('{} set global tmp_table_size'.format(container.id))
        container.exec_run(cmd='mysql --user=root --password=mysql -e "SET GLOBAL max_heap_table_size = 1024 * 1024 * 1024 * 2"')
        print('{} set global max_heap_table_size'.format(container.id))
        exit_code, output = container.exec_run(cmd='mysql --user=root --password=mysql -D av < .\{}'.format(mysqldump_filename))
        # docker exec -i 9d724ec2654e mysql --user=root --password=mysql av < .\mysql_av_20200113-040501.dump
        print('{} import dump[{}] {} {}'.format(container.id, mysqldump_filename, exit_code, output))
        """
        import_cmd = 'docker exec -i {} mysql --user=root --password=mysql av < {}'\
            .format(container_id, mysqldump_filename)
        print(import_cmd)

        return import_cmd


if __name__ == '__main__':
    mysql_backup = DropboxMysqlBackup()
    # mysqldump_filename = mysql_backup.get_today()
    # print(mysqldump_filename)

    # docker_mysql = DockerMysql()
    """
    # docker_mysql.remove_exist_mysql()
    os.chdir(".\\docker-mysql")
    cmd_docker_compose = 'docker-compose up -d'
    result_code = subprocess.run(cmd_docker_compose)
    print('docker compose result {}'.format(result_code))
    # docker_mysql.run()
    os.chdir("..")
    mysqldump_filename = 'mysql_av_20200126-040501.dump'
    import_cmd = docker_mysql.generate_mysql(mysqldump_filename)
    result_code = subprocess.run(import_cmd)
    print('import {}'.format(result_code))
    """
    # import_cmd = docker_mysql.generate_mysql('abc')

    # $ docker exec -i some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"' < /some/path/on/your/host/all-databases.sql
    # docker exec -i 79eeab242fce3499db18fe914d628e86d45448c2ad62cfae15908d735471e1bd mysql --user=root --password=mysql av < mysql_av_20200126-040501.dump
    dump_file = 'mysql_av_20200126-040501.dump'
    # 'docker exec -i 79eeab242fce3499db18fe914d628e86d45448c2ad62cfae15908d735471e1bd mysql --user=root --password=mysql av'
    cmd = 'docker exec -i 018bbbe7c77e mysql --user=root --password=mysql av < .\\mysql_av_20200126-040501.dump'
    # cmd = 'docker exec -i 2b32701f631c sh -c \'exec mysql --user=root --password=mysql av\' < .\\mysql_av_20200126-040501.dump'
    result_code = subprocess.run(cmd)
    print('import {}'.format(result_code))

    with open(dump_file, "rb") as rf:
        result_code = subprocess.run(['docker', 'exec', '-i', '018bbbe7c77e', 'sh', '-c', 'exec mysql --user=root --password=mysql av'], stdin=rf)
        print('import {}'.format(result_code))

    # with open(dump_file, "rb") as rf:
    #     result_code = subprocess.run(('docker', 'exec', '-i', '79eeab242fce3499db18fe914d628e86d45448c2ad62cfae15908d735471e1bd', 'mysql --user=root --password=mysql av < C:\\Users\\JuichiHirao\\PycharmProjects\\ServerTools\\mysql_av_20200126-040501.dump'))
    #     print('import {}'.format(result_code))
    # with open(dump_file, "rb") as rf:
    # result_code = subprocess.run(('docker', 'exec', '-i', '79eeab242fce3499db18fe914d628e86d45448c2ad62cfae15908d735471e1bd', 'mysql --user=root --password=mysql av < C:\\Users\\JuichiHirao\\PycharmProjects\\ServerTools\\mysql_av_20200126-040501.dump'))
    # print('import {}'.format(result_code))
