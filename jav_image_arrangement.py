import yaml
import mysql.connector
import os
import pathlib
import shutil
from javcore import db
from datetime import datetime, timedelta
import re


def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


class YearFileCount:
    def __get_index_month(self, month: int):
        if 1 <= month <= 4:
            return 0
        if 5 <= month <= 8:
            return 1
        if 9 <= month <= 12:
            return 2

    def execute(self):
        cnt_2018 = [0, 0, 0]
        cnt_2019 = [0, 0, 0]
        cnt_2020 = [0, 0, 0]
        cnt = 0
        for file in find_all_files('C:\mydata\jav-save'):
        # for file in find_all_files('F:\JH-STORAGE\ContentsWork\SHIZUE121114\Photos\YUI'):
            # ctime = os.path.getctime(file)
            # atime = os.path.getatime(file)
            # mtime = os.path.getmtime(file)
            p_file = pathlib.Path(file)
            p_file_stat = p_file.stat()
            mtime_dt = datetime.fromtimestamp(p_file_stat.st_mtime)
            # size = os.path.getsize(file)
            # print('dir  {} {} c {} a {} m {} size {}'.format(os.path.dirname(file), os.path.basename(file)
            #                                                  , ctime, atime, mtime, size))
            month = self.__get_index_month(mtime_dt.month)
            if mtime_dt.year == 2018:
                cnt_2018[month] = cnt_2018[month] + 1
                # cnt_2018 = cnt_2018 + 1
                continue
            elif mtime_dt.year == 2019:
                cnt_2019[month] = cnt_2019[month] + 1
                if month == 1:
                    print(p_file.name)
                # cnt_2019 = cnt_2019 + 1
                continue
            elif mtime_dt.year == 2020:
                cnt_2020[month] = cnt_2020[month] + 1
                # cnt_2020 = cnt_2020 + 1
                continue
            else:
                cnt = cnt + 1
                continue

        print('2018 files [{}]'.format(cnt_2018))
        print('2019 files [{}]'.format(cnt_2019))
        print('2020 files [{}]'.format(cnt_2020))
        print('other files [{}]'.format(cnt))


class JavImageArrangement:

    def __init__(self):
        self.jav_dao = db.jav.JavDao()
        self.jav2_dao = db.jav2.Jav2Dao()
        self.backup_dao = db.backup.BackupHistoryDao()
        self.base_dir = 'C:\\mydata'
        self.target_dirs = ['jav-save', 'jav-save2018-1']
        self.dest_base_dir = 'C:\\mydata\jav-images'

        self.is_check = False

        if not os.path.isdir(self.dest_base_dir):
            os.mkdir(self.dest_base_dir)
            print('出力先のdest_base_dirを作成しました [{}]'.format(self.dest_base_dir))

    def __get_index_month(self, month: int):
        if 1 <= month <= 4:
            return 0
        if 5 <= month <= 8:
            return 1
        if 9 <= month <= 12:
            return 2

    def __get_suffix_month(self, month: int):
        if 1 <= month <= 4:
            return '-01_04'
        if 5 <= month <= 8:
            return '-05_08'
        if 9 <= month <= 12:
            return '-09_12'

    def __get_dest_dir(self, year: int, month: int):

        if 2000 > year:
            print('想定しないpost_date.year [{}]'.format(year))
            exit(-1)
        if 1 <= month <= 12:
            print('想定しないpost_date.month [{}]'.format(month))
            exit(-1)

        dir_name = '{}{}'.format(year, self.__get_suffix_month(month))
        full_path = os.path.join(self.dest_base_dir, dir_name)
        if os.path.isdir(full_path):
            os.mkdir(full_path)
            print('出力先の年、月のフォルダを作成 [{}]'.format(full_path))

        return full_path

    def __check_dir(self, dirname):

        path = os.path.join(self.dest_base_dir, dirname)
        if not os.path.isdir(path):
            os.mkdir(path)
            print('出力先の年、月のフォルダを作成 [{}]'.format(path))

        return path

    def executeJavAndImage(self, limit: int=10):

        javs = self.jav_dao.get_where_agreement('WHERE 1 = 1 ORDER BY post_date LIMIT {}'.format(limit))

        base_date = datetime.now() - timedelta(weeks=16)
        print('{} 以前のデータを削除'.format(base_date))
        idx = 0
        for jav in javs:
            if jav.isSelection == 0 or jav.isSelection == 1 or jav.isSelection == 3:
                print('exist -1 is_selection [0, 1, 3]の、未処理のデータが存在しました [{}]'.format(jav.id))
                exit(-1)

            if jav.postDate > base_date:
                print('exist -1 基準日[{}]を超えるデータになったので、処理終了'.format(jav.postDate))
                exit(-1)

            print('[{}] 【{} < {}】 {}'.format(jav.id, jav.postDate, base_date, jav.title))
            # jav.print()
            dirname = '{}{}'.format(jav.postDate.year, self.__get_suffix_month(jav.postDate.month))
            dirname = self.__check_dir(dirname)
            package_list = self.__get_pathname(jav.package)
            # print(' p [{}]'.format(package_list))
            if len(package_list) <= 0:
                print('    none package id[{}] [{}]'.format(jav.id, jav.package))
                # exit(-1)
            thumbnail_list = self.__get_pathname(jav.thumbnail)
            # print(' t [{}]'.format(thumbnail_list))
            if len(thumbnail_list) <= 0:
                print('    none thumbnail id[{}] [{}]'.format(jav.id, jav.thumbnail))
                # exit(-1)
            result = self.backup_dao.is_exist_by_id(jav.id, 'jav_history')
            if not result:
                self.backup_dao.export_jav(jav)
                self.backup_dao.commit()
                print('    [{}] {} {} {}'.format(idx, dirname, package_list, thumbnail_list))
            else:
                print('    jav_history is_exist {}'.format(jav.id))

            self.__move_files(package_list, dirname)
            self.__move_files(thumbnail_list, dirname)

            if self.is_check is False:
                self.jav_dao.delete_by_id(jav.id)

            idx = idx + 1

    def executeJav2CreatedAt(self, limit: int = 10):
        '''
        jav2のデータはpost_dateがtext型なので、created_atで判断
        '''
        jav2s = self.jav2_dao.get_where_agreement('WHERE 1 = 1 ORDER BY created_at LIMIT {}'.format(limit))
        # jav2s = self.jav2_dao.get_where_agreement('WHERE id = 37782 ORDER BY created_at LIMIT 10000')

        base_date = datetime.now() - timedelta(weeks=16)
        print('{} 以前のデータを削除'.format(base_date))
        idx = 0
        for jav2 in jav2s:

            if jav2.createdAt is not None and jav2.createdAt > base_date:
                print('exist -1 基準日[{}]を超えるcreatedAtになったので、処理終了'.format(jav2.postDate))
                exit(-1)

            print('jav2 [{}] 【{} < {}】 {}'.format(jav2.id, jav2.createdAt, base_date, jav2.title))

            result = self.backup_dao.is_exist_by_id(jav2.id, 'jav2_history')
            if not result:
                self.backup_dao.export_jav2(jav2)
                self.backup_dao.commit()
            else:
                print('    jav2_history is_exist {}'.format(jav2.id))

            if self.is_check is False:
                self.jav2_dao.delete_by_id(jav2.id)

            idx = idx + 1

    def __move_files(self, source_file_list: list = [], dest_path: str = ''):

        for source_file in source_file_list:
            p_file = pathlib.Path(source_file)

            dest_pathname = os.path.join(dest_path, p_file.name)
            if not os.path.isfile(dest_pathname):
                shutil.copyfile(source_file, dest_pathname)
                os.remove(source_file)
                print('    移動完了 {} <- {}'.format(dest_pathname, source_file))
            # else:
            #     print('コピー先のファイル存在 [{}]'.format(source_file))

    def __get_pathname(self, filename):
        if filename is None or len(filename.strip()) <= 0:
            return []
        file_list = filename.split(' ')
        # file_stat_list = []
        pathname_list = []

        for file in file_list:
            if ':' in file:
                p_file = pathlib.Path(filename)
                file = p_file.name
            for dir in self.target_dirs:
                full_dir = os.path.join(self.base_dir, dir)
                pathname = os.path.join(full_dir, file)
                if os.path.isfile(pathname):
                    pathname_list.append(pathname)
                    break

        if len(pathname_list) <= 0:
            print('    ファイル無し {}'.format(filename))

        # return file_stat_list
        return pathname_list


if __name__ == '__main__':
    jav_image_arrangement = JavImageArrangement()
    # jav_image_arrangement.executeJavAndImage(1000)
    jav_image_arrangement.executeJav2CreatedAt(50000)
    # year_file_count = YearFileCount()
    # year_file_count.execute()
