from datetime import datetime

import openpyxl

start_time = datetime.now()

new_file = openpyxl.Workbook()
new_sheet = new_file.active


for row in range(1, 3800):
    for col in range(1, 100):
        new_sheet.cell(row=row, column=col, value="hodnota")


new_file.save(
    "/Users/Zbynek/Documents/MyDocuments/BDD/projekty/Nibbler/celus-nibbler/celus_nibbler/convertors/bigfile.xlsx"
)
new_file.close()

end_time = datetime.now()
duration = end_time - start_time


print(f"duration: {duration}")
