import csv

from celus_nibbler.convertors.openpyxl_convertor import openpyxl_convertor
from celus_nibbler.convertors.pandas_convertor import pandas_convertor
from celus_nibbler.convertors.pyexcel_convertor import pyexcel_convertor

my_files = [
    "smallfile_27KB",
    "smallfile_116KB",
    "smallfile_205KB",
    "smallfile_383KB",
    "smallfile_850KB",
    "bigfile_1-2MB",
    "bigfile_2-3MB",
    "bigfile_3-4MB",
    "bigfile_4-5MB",
    # "bigfile_5-6MB",
    # "bigfile_7MB",
    # "bigfile_10MB",
    # "bigfile_11-12MB",
]


outcome_file = "/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/outcomes.csv"

with open(outcome_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(
        [
            "file name",
            "file format",
            "file size in MB",
            "tool",
            "memory_usage after sheet loaded",
            "memory_usage after sheet converted",
            "duration",
        ]
    )
    for my_file in my_files:
        outcomes = openpyxl_convertor(my_file)
        writer.writerow(
            [
                outcomes[0],
                outcomes[1],
                outcomes[2],
                outcomes[3],
                outcomes[4],
                outcomes[5],
                outcomes[6],
            ]
        )
        outcomes = pandas_convertor(my_file)
        writer.writerow(
            [
                outcomes[0],
                outcomes[1],
                outcomes[2],
                outcomes[3],
                outcomes[4],
                outcomes[5],
                outcomes[6],
            ]
        )
        outcomes = pyexcel_convertor(my_file)
        writer.writerow(
            [
                outcomes[0],
                outcomes[1],
                outcomes[2],
                outcomes[3],
                outcomes[4],
                outcomes[5],
                outcomes[6],
            ]
        )
