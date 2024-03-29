from io import BytesIO, StringIO

import pytest

from celus_nibbler.reader import CsvSheetReader


@pytest.fixture
def sheet_csv():
    return StringIO("Name,Values\nFirst,1\nSecond,2\nThird,3\nFourth,4\n")


@pytest.fixture
def csv_sheet_reader(sheet_csv):
    return CsvSheetReader(0, None, sheet_csv)


@pytest.fixture
def csv_sheet_generator():
    def gen(raw_data):
        return CsvSheetReader(0, None, StringIO(raw_data))

    return gen


@pytest.fixture()
def sheet_json():
    return BytesIO(
        b"""\
{
  "Report_Header": {
    "Created": "2020-01-01T00:00:00Z",
    "Created_By": "My services",
    "Customer_ID": "88888888",
    "Report_ID": "DR",
    "Release": "5",
    "Report_Name": "Database Master Report",
    "Institution_Name": "My institution",
    "Institution_ID": [
      {
        "Type": "Proprietary",
        "Value": "IIIIIIIII:88888888"
      }
    ],
    "Report_Filters": [
      {
        "Name": "Begin_Date",
        "Value": "2020-01-01"
      },
      {
        "Name": "End_Date",
        "Value": "2020-01-31"
      }
    ],
    "Report_Attributes": [
      {
        "Name": "Attributes_To_Show",
        "Value": "Data_Type|Access_Method"
      }
    ]
  },
  "Report_Items": [
    {
      "Platform": "MyPlatform",
      "Item_ID": [
        {
          "Type": "Proprietary",
          "Value": "IIIIIIIII:000"
        }
      ],
      "Database": "Database1",
      "Publisher": "MyPublisher",
      "Data_Type": "Database",
      "Access_Method": "Regular",
      "Performance": [
        {
          "Period": {
            "Begin_Date": "2020-01-01",
            "End_Date": "2020-01-31"
          },
          "Instance": [
            {
              "Metric_Type": "Searches_Regular",
              "Count": 5
            }
          ]
        }
      ]
    },
    {
      "Platform": "MyPlatform",
      "Item_ID": [
        {
          "Type": "Proprietary",
          "Value": "IIIIIIIII:111"
        }
      ],
      "Database": "Database2",
      "Publisher": "MyPublisher",
      "Data_Type": "Book",
      "Access_Method": "Regular",
      "Performance": [
        {
          "Period": {
            "Begin_Date": "2020-01-01",
            "End_Date": "2020-01-31"
          },
          "Instance": [
            {
              "Metric_Type": "Total_Item_Investigations",
              "Count": 25
            },
            {
              "Metric_Type": "Total_Item_Requests",
              "Count": 13
            },
            {
              "Metric_Type": "Unique_Item_Investigations",
              "Count": 14
            },
            {
              "Metric_Type": "Unique_Item_Requests",
              "Count": 11
            },
            {
              "Metric_Type": "Unique_Title_Investigations",
              "Count": 13
            },
            {
              "Metric_Type": "Unique_Title_Requests",
              "Count": 11
            }
          ]
        }
      ]
    },
    {
      "Platform": "MyPlatform",
      "Title": "Title1",
      "Item_ID": [
        {
          "Type": "Print_ISSN",
          "Value": "9999-9999"
        },
        {
          "Type": "Proprietary",
          "Value": "IIIIIIIII:222"
        }
      ],
      "Database": "Database3",
      "Publisher": "MyPublisher",
      "Data_Type": "Other",
      "Section_Type": "Article",
      "YOP": "2018",
      "Access_Type": "Controlled",
      "Access_Method": "Regular",
      "Performance": [
        {
          "Period": {
            "Begin_Date": "2020-01-01",
            "End_Date": "2020-01-31"
          },
          "Instance": [
            {
              "Metric_Type": "Unique_Item_Investigations",
              "Count": 9
            }
          ]
        }
      ]
    }
  ]
}
"""
    )
