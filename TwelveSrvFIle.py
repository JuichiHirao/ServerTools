import yaml
import mysql.connector
import os
import pathlib
from datetime import datetime
import re


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

        self.cursor = self.conn.cursor()

    def get_conn(self):

        with open('credentials.yml') as file:
            obj = yaml.load(file, Loader=yaml.FullLoader)
            self.user = obj['user']
            self.password = obj['password']
            self.hostname = obj['hostname']
            self.dbname = obj['dbname']

        return mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.hostname, database='av_file')

    def is_file_exist(self, drive, path, name):
        sql = 'SELECT id FROM av_file.real_file ' \
              'WHERE drive = %s and name = %s and path = %s '

        self.cursor.execute(sql, (drive, str(name), str(path)))

        rs = self.cursor.fetchall()

        for row in rs:
            return True

        return False

    def export_file(self, drive, path, pathlib_info):
        sql = 'INSERT INTO av_file.real_file(' \
              'name, path, extension, ctime' \
              ', mtime, atime, size, drive) ' \
              'VALUES(%s, %s, %s, %s' \
              ', %s, %s, %s)'

        p_file_stat = pathlib_info.stat()

        # 作成日時
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        # 更新日時
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)

        self.cursor.execute(sql, (str(pathlib_info.name), path
                                  , pathlib_info.suffix, ctime_dt
                                  , mtime_dt, atime_dt, p_file_stat.st_size, drive))
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

    """
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
    """

    if p_file.is_file():
        m_drive = re.search('^[C-Z]{1}:', str(p_file.parent))
        if m_drive:
            drive_str = m_drive.group()
            drive_less_path = str((p_file.parent)).replace(drive_str, '')
            drive_str = drive_str.replace(':', '')
        else:
            drive_str = ''
            drive_less_path = ''
        ctime_dt = datetime.fromtimestamp(p_file_stat.st_ctime)
        atime_dt = datetime.fromtimestamp(p_file_stat.st_atime)
        mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)
        # print('{} file {} {} c {} a {} m {} size {}'.format(drive_str, drive_less_path, p_file.name
        #                                                     , ctime_dt, atime_dt, mtime_dt, p_file_stat.st_size))
        if db_dao.is_file_exist(drive_str, drive_less_path, p_file.name):
            print('exist file {} [{}]'.format(p_file.parent, p_file.name))
        else:
            print('{} file {} {} c {} a {} m {} size {}'.format(drive_str, drive_less_path, p_file.name
                                                                , ctime_dt, atime_dt, mtime_dt, p_file_stat.st_size))
            db_dao.export_file(drive_str, drive_less_path, p_file)

    idx = idx + 1
    if idx > 10:
        break

