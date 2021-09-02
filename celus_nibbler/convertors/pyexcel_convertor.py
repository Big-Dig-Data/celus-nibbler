import os
from datetime import datetime

import pyexcel

from celus_nibbler.convertors.memory_usage import log_memory

my_file = 'beckonline'
my_format = 'xlsx'
tool = 'pyexcel'


print(20 * '*' + f"  {tool}  " + 20 * '*')


print(f"filename: {my_file}")
print(f"format: {my_format}")


start_time = datetime.now()

log_memory('load file, create file and save the file')
pyexcel.save_as(
    file_name=f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}",
    dest_file_name=f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_format}_format_export_by_{tool}.csv",
)
log_memory('loading the file, creating file and saving the file done')


end_time = datetime.now()
duration = end_time - start_time


print(f"duration: {duration}")

file_size = os.stat(
    f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
).st_size
print(f"filesize: {file_size} bites")
