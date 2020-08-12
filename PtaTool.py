import openpyxl
from datetime import datetime


class AnswerData:
    def __init__(self):
        self.child_name = ''
        self.parent_name = ''
        self.mail = ''
        self.tel_no = ''
        self.watching = ''
        self.event = ''
        self.one_week = []
        self.two_week = []
        self.aug24AM = ''
        self.aug25AM = ''
        self.aug25PM = ''
        self.aug26AM = ''
        self.aug26PM = ''
        self.aug27AM = ''
        self.aug27PM = ''
        self.aug28AM = ''
        self.aug31AM = ''
        self.aug31PM = ''
        self.sep01AM = ''
        self.sep01PM = ''
        self.sep02AM = ''
        self.sep02PM = ''
        self.sep03AM = ''
        self.sep03PM = ''
        self.sep04AM = ''
        self.sep04PM = ''


class PtaTool:

    def __init__(self):
        self.excel_file = 'C:\\Users\\JuichiHirao\\Dropbox\\jhdata\\(令和2年度)PTA係作業希望調査（集計)3.xlsx'
        self.data_list = []
        self.already_list = []

    def get_data(self, place, idx):
        filter_list = list(filter(lambda answer_data:
                                  answer_data.one_week[idx] == place
                                  and '夏休み明け' in answer_data.watching, self.data_list))
        # print(filter_list)
        return filter_list

    def choice(self, select_list):
        for select_data in select_list:
            filter_list = list(filter(lambda answer_data:
                                      answer_data.parent_name == select_data.parent_name, self.already_list))
            if filter_list is not None:
                if len(filter_list) > 0:
                    continue
                else:
                    self.already_list.append(select_data)
                    return select_data
        # print(filter_list)

        return None

    def export(self):

        # [8/24(月)8:05～8:25]
        col_watching = 6
        col_0824_AM = 8
        col_0831_AM = 16
        wb = openpyxl.load_workbook(self.excel_file)
        ws = wb['original']
        rows = ws['D2':'AJ181']
        for row_idx, row in enumerate(rows):
            one_data = AnswerData()
            one_data.parent_name = row[3].value
            one_data.watching = row[col_watching].value
            one_data.event = row[col_watching+1].value
            one_data.one_week.append(row[col_0824_AM].value)
            one_data.one_week.append(row[col_0824_AM+1].value)
            one_data.one_week.append(row[col_0824_AM+2].value)
            one_data.one_week.append(row[col_0824_AM+3].value)
            one_data.one_week.append(row[col_0824_AM+4].value)
            one_data.one_week.append(row[col_0824_AM+5].value)
            one_data.one_week.append(row[col_0824_AM+6].value)
            one_data.one_week.append(row[col_0824_AM+7].value)
            one_data.aug24AM = row[col_0824_AM].value
            one_data.aug25AM = row[col_0824_AM+1].value
            one_data.aug25PM = row[col_0824_AM+2].value
            one_data.aug26AM = row[col_0824_AM+3].value
            one_data.aug26PM = row[col_0824_AM+4].value
            one_data.aug27AM = row[col_0824_AM+5].value
            one_data.aug27PM = row[col_0824_AM+6].value
            one_data.aug28AM = row[col_0824_AM+7].value
            one_data.aug31AM = row[col_0831_AM].value
            one_data.sep01AM = row[col_0831_AM+1].value
            one_data.sep01PM = row[col_0831_AM+2].value
            one_data.sep02AM = row[col_0831_AM+3].value
            one_data.sep02PM = row[col_0831_AM+4].value
            one_data.sep03AM = row[col_0831_AM+5].value
            one_data.sep03PM = row[col_0831_AM+6].value
            one_data.sep04AM = row[col_0831_AM+7].value
            one_data.sep04PM = row[col_0831_AM+8].value

            self.data_list.append(one_data)

        place_list = ['白山神社前', '鵜の木幼稚園前', '増明院前', '久が原駅前', 'ぬめり坂上']
        datetime_list = ['8/24 AM', '8/25 AM', '8/25 PM', '8/26 AM', '8/26 PM', '8/27 AM', '8/27 PM', '8/28 AM']
        for idx in range(8):
            print('{}'.format(datetime_list[idx]))
            for place in place_list:
                result_list = self.get_data(place, idx)
                choise_data = self.choice(result_list)
                if choise_data is not None:
                    print('  {} {}件 {}'.format(place, len(result_list), choise_data.parent_name))
                else:
                    print('  {} {}件 None'.format(place, len(result_list)))

        """
        result = self.get_data('白山神社前', 0)
        print('{}件 {}'.format(len(result), result[0].parent_name))
        result = self.get_data('鵜の木幼稚園前', 0)
        print('{}件 {}'.format(len(result), result[0].parent_name))
        result = self.get_data('増明院前', 0)
        print(result[0].parent_name)
        result = self.get_data('久が原駅前', 0)
        print(result[0].parent_name)
        result = self.get_data('ぬめり坂上', 0)
        print(result[0].parent_name)
        """

        # print(len(result))
        # for one_data in data_list:
        #     print(one_data.aug24AM)
        #     print(one_data.sep01AM)

if __name__ == '__main__':
    tv_contents_register = PtaTool()
    tv_contents_register.export()
