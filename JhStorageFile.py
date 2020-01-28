import yaml
import mysql.connector
import os
import pathlib
from datetime import datetime


def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


class MysqlBase:

    def __init__(self, table_name=''):
        self.max_time = 0
        self.user = ''
        self.password = ''
        self.hostname = ''
        self.dbname = ''
        self.cursor = None

        self.conn = self.get_conn()

        # テーブル名が指定されていた場合は取得済みの回数を設定
        if len(table_name) > 0:
            self.table_name = table_name
            max_time = self.db.prepare("SELECT max(times) FROM " + table_name)
            for row in max_time():
                self.max_time = int(row[0])

        self.cursor = self.conn.cursor()

    def get_conn(self):

        with open('credentials.yml') as file:
            obj = yaml.load(file, Loader=yaml.FullLoader)
            self.user = obj['user']
            self.password = obj['password']
            self.hostname = obj['hostname']
            self.dbname = obj['dbname']

        return mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.hostname, database=self.dbname)

    def is_file_exist(self, path, name):
        sql = 'SELECT id FROM tv.real_file ' \
              'WHERE name = %s and path = %s '

        self.cursor.execute(sql, (str(name), str(path)))

        rs = self.cursor.fetchall()

        for row in rs:
            return True

        return False

    def export_file(self, pathlib_info):
        sql = 'INSERT INTO tv.real_file(' \
              'name, path, extension, ctime' \
              ', mtime, atime, size) ' \
              'VALUES(%s, %s, %s, %s' \
              ', %s, %s, %s)'

        p_file_stat = pathlib_info.stat()

        # 作成日時
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        # 更新日時
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)

        self.cursor.execute(sql, (str(pathlib_info.name), str(pathlib_info.parent)
                                  , pathlib_info.suffix, ctime_dt
                                  , mtime_dt, atime_dt, p_file_stat.st_size))
        self.conn.commit()

    def is_dir_exist(self, path, name):
        sql = 'SELECT id FROM tv.real_dir ' \
              'WHERE name = %s and path = %s '

        self.cursor.execute(sql, (str(name), str(path)))

        rs = self.cursor.fetchall()

        for row in rs:
            return True

        return False

    def export_dir(self, pathlib_info):
        sql = 'INSERT INTO tv.real_dir( ' \
              'name, path, ctime, mtime ' \
              ', atime) ' \
              'VALUES(%s, %s, %s, %s, %s)'

        p_file_stat = pathlib_info.stat()

        # 作成日時
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        # 更新日時
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)

        self.cursor.execute(sql, (str(pathlib_info.name), str(pathlib_info.parent)
                                  , ctime_dt, mtime_dt, atime_dt))
        self.conn.commit()


db_dao = MysqlBase()
idx = 0
for file in find_all_files('F:\JH-STORAGE'):
# for file in find_all_files('F:\JH-STORAGE\ContentsWork\SHIZUE121114\Photos\YUI'):
    # ctime = os.path.getctime(file)
    # atime = os.path.getatime(file)
    # mtime = os.path.getmtime(file)
    # size = os.path.getsize(file)
    # print('dir  {} {} c {} a {} m {} size {}'.format(os.path.dirname(file), os.path.basename(file)
    #                                                  , ctime, atime, mtime, size))
    p_file = pathlib.Path(file)
    p_file_stat = p_file.stat()
    # file_str = 'F:\JH-STORAGE\ContentsWork\SHIZUE121114\Photos\YUI'
    if 'JPG' in str(p_file.suffix).upper():
        continue

    if p_file.is_dir():
        # 作成日時
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        # 更新日時
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)
        print('dir  {} {} c {} a {} m {} size {}'.format(p_file.parent, p_file.name
                                                         , ctime_dt, atime_dt, mtime_dt, p_file_stat.st_size))
        if db_dao.is_dir_exist(p_file.parent, p_file.name):
            print('exist dir {} [{}]'.format(p_file.parent, p_file.name))
        else:
            db_dao.export_dir(p_file)

    elif p_file.is_file():
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)
        print('file {} {} c {} a {} m {} size {}'.format(p_file.parent, p_file.name
                                                         , ctime_dt, atime_dt, mtime_dt, p_file_stat.st_size))
        if db_dao.is_file_exist(p_file.parent, p_file.name):
            print('exist file {} [{}]'.format(p_file.parent, p_file.name))
        else:
            db_dao.export_file(p_file)

    idx = idx + 1
    # if idx > 5:
    #     break


