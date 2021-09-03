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

    start_time = datetime.now()

    excel = openpyxl.load_workbook(
        filename=f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}",
        read_only=read_only,
        data_only=True,
        keep_links=False,
    )

    sheet = excel.active
    sheet_loaded_memory_usage = log_memory()

    col = csv.writer(
        open(
            f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}_export_by_{tool}.csv",
            'w',
            newline="",
        )
    )

    for r in sheet.rows:
        col.writerow([cell.value for cell in r])

    excel.close()
    sheet_converted_memory_usage = log_memory()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"duration: {duration}")
    file_size = os.stat(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
    ).st_size

    file_size_in_MB = file_size / 1000000
    return (
        my_file,
        my_format,
        file_size_in_MB,
        tool,
        sheet_loaded_memory_usage,
        sheet_converted_memory_usage,
        duration,
    )
