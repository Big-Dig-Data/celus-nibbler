import csv
import re

import openpyxl


def convert(file, file_type, tmp_file):
    if re.search('CSV', file_type):
        return file
    elif re.search('Microsoft Excel 2007+', file_type):
        return xlsx_to_csv(file, tmp_file)
    elif re.search('Composite Document File V2 Document', file_type):
        # xls'
        pass
    elif re.search('ASCII text', file_type):
        #  tsv'
        pass
    else:
        pass
        # add an Exception here


def xlsx_to_csv(file, tmp_file):

    excel = openpyxl.load_workbook(filename=file, read_only=True, data_only=True, keep_links=False)
    sheet = excel.active
    csv_file = csv.writer(tmp_file)
    for r in sheet.rows:
        csv_file.writerow([cell.value for cell in r])
    excel.close()
    return csv_file


# # create a temporary directory using the context manager
# >>> with tempfile.TemporaryDirectory() as tmpdirname:
# ...     print('created temporary directory', tmpdirname)
# >>>
# # directory and contents have been removed
