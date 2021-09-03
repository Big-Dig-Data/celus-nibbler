import os
from datetime import datetime

import pandas as pd

from celus_nibbler.convertors.memory_usage import log_memory


def pandas_convertor(my_file):

    my_format = 'xlsx'
    tool = 'pandas'

    print(20 * '*' + f"  {tool}  " + 20 * '*')

    print(f"filename: {my_file}")

    start_time = datetime.now()

    read_file = pd.read_excel(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}"
    )

    sheet_loaded_memory_usage = log_memory()

    read_file.to_csv(
        f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}_export_by_{tool}.csv",
        index=None,
        header=False,
    )
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
