import os
import pathlib
import sys
from javcore import db
from datetime import datetime
from moviepy.editor import VideoFileClip


def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


class TvFileData:

    def __init__(self):

        self.id = -1
        self.contentsId = -1
        self.detailId = -1
        self.storeId = -1
        self.label = ''
        self.name = ''
        self.source = ''
        self.duration = 0
        self.time = ''
        self.videoInfo = ''
        self.comment = ''
        self.size = 0
        self.priorityNUmber = 0
        self.fileDate = None
        self.fileStatus = 'exist'
        self.quality = 0
        self.remark = ''
        self.rating1 = 0
        self.rating2 = 0
        self.createdAt = None
        self.updatedAt = None

    def set_time(self):

        hour = 0
        min = 0
        sec = 0
        if self.duration <= 0:
            return
        hour = self.duration // 3600
        min = self.duration // 60
        sec = self.duration % 60

        time_str = ''
        if hour > 0:
            time_str = '{}h'.format(hour)
        if min > 0:
            time_str = '{}{}m'.format(time_str, min)

        time_str = '{}{}s'.format(time_str, str(sec).zfill(2))

        self.time = time_str
        return


    def print(self):
        print('【{}】 '.format(self.name))
        print('  id {} c_id {} store_id {}'.format(self.id, self.contentsId, self.storeId))
        print('  label {}'.format(self.label))
        print('  source [{}] duration [{}]->[{}]'.format(self.source, self.duration, self.time))
        print('  v_info {} quality {}'.format(self.videoInfo, self.quality))
        print('  size {}'.format(self.size))
        print('  file_date {}, file_status {}'.format(self.fileDate, self.fileStatus))
        print('  r1 {} r2 {}'.format(self.rating1, self.rating2))
        print('  comment {}'.format(self.comment))
        print('  createdAt {}  updatedAt {}'.format(self.createdAt, self.updatedAt))


class TvFileDao(db.mysql_base.MysqlBase):

    def get_dir_id(self, path: str = ''):
        sql = 'SELECT id FROM tv.real_dir WHERE path = %s '

        self.cursor.execute(sql, (path,))

        rs = self.cursor.fetchall()

        for row in rs:
            return row[0]

        return -1

    def get_where_list(self, where_sql: str = '', param_list: list = []):

        sql = 'SELECT id' \
              '  , contents_id, detail_id, store_id, label ' \
              '  , `name`, source, duration, `time` ' \
              '  , video_info, comment, `size`, priority_num ' \
              '  , file_date, file_status,quality, remark ' \
              '  , rating1, rating2 ' \
              '  , created_at, updated_at ' \
              '  FROM tv.file '

        if len(where_sql) > 0:
            sql = '{} {}'.format(sql, where_sql)
            self.cursor.execute(sql, param_list)
        else:
            self.cursor.execute(sql)

        rs = self.cursor.fetchall()

        data_list = []
        for row in rs:
            data = TvFileData()
            data.id = row[0]
            data.contentsId = row[1]
            data.detailId = row[2]
            data.storeId = row[3]
            data.label = row[4]
            data.name = row[5]
            data.source = row[6]
            data.duration = row[7]
            data.time = row[8]
            data.videoInfo = row[9]
            data.comment = row[10]
            data.size = row[11]
            data.priorityNUmber = row[12]
            data.fileDate = row[13]
            data.fileStatus = row[14]
            data.quality = row[15]
            data.remark = row[16]
            data.rating1 = row[17]
            data.rating2 = row[18]
            data.createdAt = row[19]
            data.updatedAt = row[20]

            data_list.append(data)

        return data_list

    def is_exist(self, filename: str = ''):

        sql = 'SELECT id FROM tv.file WHERE name = %s '

        self.cursor.execute(sql, (filename,))

        rs = self.cursor.fetchall()

        for row in rs:
            return True

        return False

    def update_time(self, file_data: TvFileData = None):

        if file_data is None:
            return

        sql = 'UPDATE tv.file ' \
              '  SET time = %s ' \
              '  WHERE id = %s '

        self.cursor.execute(sql, (file_data.time, file_data.id))

        self.conn.commit()

    def export(self, file_data: TvFileData = None):

        if file_data is None:
            return

        sql = 'INSERT INTO tv.file (store_id' \
              '  , label, `name`, source, duration ' \
              '  , `time`, video_info, comment, `size` ' \
              '  , file_date, file_status ' \
              '  ) ' \
              ' VALUES(%s ' \
              '  , %s, %s, %s, %s' \
              '  , %s, %s, %s, %s' \
              '  , %s, %s ' \
              ' )'

        self.cursor.execute(sql, (file_data.storeId
                                  , file_data.label, file_data.name, file_data.source, file_data.duration
                                  , file_data.time, file_data.videoInfo, file_data.comment, file_data.size
                                  , file_data.fileDate, file_data.fileStatus)
                            )

        self.conn.commit()


class TvContentsRegister:

    def __init__(self):
        self.tv_file_dao = TvFileDao()
        # self.base_dir = 'M:\\TV_CONTENTS'
        self.base_dir = 'F:\\TVREC'

        # self.is_check = True
        self.is_check = False

    def __get_duration(self, pathname: str = ''):

        duration = 0
        try:
            clip = VideoFileClip(pathname)
            duration = int(clip.duration)
        except:
            print(sys.exc_info())

        return duration

    def execute(self):

        dir_id = self.tv_file_dao.get_dir_id(self.base_dir)
        print(dir_id)
        if dir_id <= 0:
            print('store_idに存在しないパスが設定されました {}'.format(self.base_dir))
            exit(-1)
        for file in find_all_files(self.base_dir):

            p_file = pathlib.Path(file)
            if p_file.is_dir():
                continue

            if self.tv_file_dao.is_exist(p_file.name):
                print('is_exist {}'.format(p_file.name))
                continue

            data = TvFileData()
            data.duration = self.__get_duration(str(p_file.resolve()))
            data.storeId = dir_id
            data.name = p_file.name
            # data.label = 'MEDIA2020'
            data.label = 'MEDIA2018-TVREC'
            data.source = 'my'
            p_file_stat = p_file.stat()
            data.size = p_file_stat.st_size
            data.fileDate = datetime.fromtimestamp(p_file_stat.st_mtime)
            data.set_time()
            data.print()
            if self.is_check is False:
                self.tv_file_dao.export(data)

            # idx = idx + 1

    def update_target_duration(self):
        data_list = self.tv_file_dao.get_where_list('WHERE time is null and file_date >= "2020-06-01"')
        for data in data_list:
            data.set_time()
            self.tv_file_dao.update_time(data)
            data.print()


if __name__ == '__main__':
    tv_contents_register = TvContentsRegister()
    tv_contents_register.execute()
    # tv_contents_register.update_target_duration()
