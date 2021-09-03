import os
from datetime import datetime

import pandas as pd

from celus_nibbler.convertors.memory_usage import log_memory


def pandas_convertor(my_file):

    my_format = 'xlsx'
    tool = 'pandas'

    print(20 * '*' + f"  {tool}  " + 20 * '*')

    print(f"filename: {my_file}")
    print(f"format: {my_format}")

    start_time = datetime.now()

    log_memory('read file')
    read_file = pd.read_excel(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
    )
    log_memory('file read')

    read_file.to_csv(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}_export_by_{tool}.csv",
        index=None,
        header=False,
    )
    log_memory('csv file created and written')

    end_time = datetime.now()
    duration = end_time - start_time

    print(f"duration: {duration}")

    file_size = os.stat(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
    ).st_size
    if file_size < 1000000:
        file_size_in_KB = file_size / 1000
        print(f"filesize: {file_size_in_KB} KB")
    else:
        file_size_in_MB = file_size / 1000000
        print(f"filesize: {file_size_in_MB} MB")
