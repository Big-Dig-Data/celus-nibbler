import csv
import os
from datetime import datetime

import xlrd

from celus_nibbler.convertors.memory_usage import log_memory

my_file = 'Brepolis_Medieval'
my_format = 'xls'
tool = 'xlrd'

print(20 * '*' + f"  {tool}  " + 20 * '*')


print(f"filename: {my_file}")
print(f"format: {my_format}")
start_time = datetime.now()
log_memory('load workbook')
with xlrd.open_workbook(
    f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
) as wb:
    log_memory('workbook loaded')
    sh = wb.sheet_by_index(0)  # wb.sheet_by_name('sheet_name')
    with open(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_format}_format_export_by_{tool}.csv",
        'w',
        newline="",
    ) as f:
        log_memory('csv file created and opened')
        col = csv.writer(f)
        for row in range(sh.nrows):
            col.writerow(sh.row_values(row))
        log_memory('values written in csv file')


end_time = datetime.now()
duration = end_time - start_time


print(f"duration: {duration}")

file_size = os.stat(
    f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
).st_size
print(f"filesize: {file_size} bites")
