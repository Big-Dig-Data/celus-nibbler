from celus_nibbler.convertors.openpyxl_convertor import openpyxl_convertor
from celus_nibbler.convertors.pandas_convertor import pandas_convertor
from celus_nibbler.convertors.pyexcel_convertor import pyexcel_convertor

my_files = [
    "bigfile_1-2MB",
    "bigfile_2-3MB",
    "bigfile_3-4MB",
    "bigfile_4-5MB",
    "bigfile_5-6MB",
    "bigfile_7MB",
    # "bigfile_10MB",
    # "bigfile_11-12MB",
]

for my_file in my_files:
    openpyxl_convertor(my_file)
    pandas_convertor(my_file)
    pyexcel_convertor(my_file)
