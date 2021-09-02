from datetime import datetime

import pandas as pd

start_time = datetime.now()

my_file = 'Anne'
my_format = 'xlsx'
read_file = pd.read_excel(
    f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_file}.{my_format}",
    engine="openpyxl",
)

read_file.to_csv(
    f"/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/{my_format}_format_export_by_pandas.csv",
    index=None,
    header=True,
)


end_time = datetime.now()
duration = end_time - start_time

print(f"format: {my_format}")
print(f"filename: {my_file}")
print("pandas")
print(f"duration: {duration}")
