import csv
import os
from datetime import datetime

import openpyxl

from celus_nibbler.convertors.memory_usage import log_memory


def openpyxl_convertor(my_file):

    my_format = 'xlsx'
    read_only = True
    tool = 'openpyxl'

    print(20 * '*' + f"  {tool}  " + 20 * '*')

    print(f"filename: {my_file}")
    print(f"format: {my_format}")
    print(f"read_only={read_only}")

    start_time = datetime.now()

    log_memory('load workbook')
    excel = openpyxl.load_workbook(
        filename=f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}",
        read_only=read_only,
        data_only=True,
        keep_links=False,
    )
    log_memory('workbook loaded')
    log_memory('fidnd active excel sheet')
    sheet = excel.active

    log_memory('active excel sheet found')

    col = csv.writer(
        open(
            f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}_export_by_{tool}.csv",
            'w',
            newline="",
        )
    )
    log_memory('csv file created and opened')

    for r in sheet.rows:
        col.writerow([cell.value for cell in r])
    log_memory('values written in csv file')
    excel.close()
    log_memory('excel file closed')
    end_time = datetime.now()
    duration = end_time - start_time

    print(f"duration: {duration}")

    file_size = os.stat(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
    ).st_size
    print(f"filesize: {file_size} bites")
