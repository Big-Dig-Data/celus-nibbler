import csv
from datetime import datetime

import openpyxl

my_file = 'Anne'
my_format = 'xlsx'
read_only = True

start_time = datetime.now()
excel = openpyxl.load_workbook(
    filename=f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}",
    read_only=read_only,
    data_only=True,
    keep_links=False,
)


sheet = excel.active


col = csv.writer(
    open(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_format}_format_export_by_openpyxl.csv",
        'w',
        newline="",
    )
)


for r in sheet.rows:
    col.writerow([cell.value for cell in r])

excel.close()

end_time = datetime.now()
duration = end_time - start_time

print(f"format: {my_format}")
print(f"filename: {my_file}")
print("openpyxl")
print(f"duration: {duration}")
print(f"read_only={read_only}")
