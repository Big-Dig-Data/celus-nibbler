# Celus Nibbler Documentation

## Requirements

- [] there should not be a parser class which serves to two tables which both are having different dimensions.
- each type of non-counter report has its own parser

## Terminology
- **TableReader**: reads uploaded file which might be in either .csv format (`CsvReader()`), .xlsx format (`XlsxReader()`) and in the future possibly .xls format (`XlsReader()`) and creates standard format `read_table` (list of lists) which is list of rows with values.
- **Parser**: parses standard format `read_table` (list of lists) created by TableReader and creates a CounterReport from it.


## Parsers

- each parser carries:
    - list of Publishers from whom we register particular type of non-counter report.
        - if Publisher not in the list parser wont parse the report.
    - list of metrics we expect from particular type of non-counter report.
        - if metric not in the list parser will parse the report and store the new metric in a list.

- how Nibbler recognizes the correct parser for the non-counter report:
    1. checks if the report is from expected Publisher (a.k.a. `platform`).
    2. checks for specific heuristic:
      - any values expected at certain places in the report. Each parser has multiple of those checks.
    3. checks for expected title of column with metrics


- each parser checks wheather its getting expected metrics according to the list of metrics it carries.
        - if metric not in the list of metrics the parser will parse the report with message about registering new metric and stores this new metric in its list of expected metrics.

- validations even during parsing

### class RelatedTo(Enum)

This class instructs the parser how to iterate over the variable (titles, metrics, etc.) and assign values he finds in it to each record.

- `TABLE` -  the one value in this variable goes to each record in a table
- `ROW`  -  iterate over this variable and assign each value in it to every record in a row
- `COL`  -  iterate over this variable and assign each value in it to every record in a column
- `FIELD`   -   iterate over this variable and assign each value in it to only one record at the time

**For more explanation:** there are tables which have only one mentioning of "title" for the whole table, therefore this value "title" will be specified as `['title']['occurrence'] = RelatedTo.TABLE` so that the parser will know that the value in variable "title" should be assigned to each record in this table without any iteration over this variable. There are tables which have titles in each row, thus in these cases title will have `RelatedTo.ROW` and the parser will know to iterate over the "title" variable for each report in a row. Other values have `RelatedTo.COL` (those are months mostly) and in this case parser knows that it should iterate over this variable in a matter that it assigns its values to each report in a column. There are values which has its own mentioning per value (those are mostly values obviously, but in some table types those can be also "title" or "metric" values and others) so these values will have `RelatedTo.FIELD` and parser will know that it should assign this value only to one report at the time.

The same architecture and logic will apply to metrics, months, and to any dimension data we will encounter. All of these values can be mentioned in various tables for each row or for each col, or even for each value, and some of then  one for whole table - using this we will have an easy way to explain to any parser how to assign these values to each record.
## Exceptions
- if wrong parser is used for a report, it tells in what place validation failed and how it failed.

## Tests
- `test_findparser()` - going through all tables in directory `celus-nibbler/tests/data/csv/<parser_name>` and tests whether `findparser()` assigns for each table a parser which name corresponds with directory `<parser_name>` in which the table is located.
- `test_eat()` - goes through each item in `testing_data.data` and checks wheter `eat()` finds the exact parser and parses the exact counter-report as mentioned in `testing_data.data`.

### naming of test files
- rule for naming test files is: `<firstsheet_parser>-<secondsheet_parser>-<nsheet-parser>-<unique_letters>.sufix`
- please notice the `'-'` which are important to distiguish parsers in the filename.
- `'0'` means:  this sheet has no data to be parsed (for example: file `1_2_1-0-2_2_1-gkr.csv` has in first sheet data to be parsed by parser `1_2_1`, in second sheet there are no data to be parsed, third sheet has data to be parsed by parser `2_2_1`)


## Formats

each parser recognizes corresponding table to parse by matching its `heuristics` and table platform is in parsers `platforms`. See class attributes of each parser in `celus_nibbler/parsers/format_....py` respectively.


***
### Format_1_3_1
#### characteristics:
```
platforms = [
        'Naxos',
        'CHBeck',
        'Knovel',
        'Uptodate',
        'SciFinder',
        'SciVal',
    ]
```



#### examples:
![example1](img/Format_1_3_1(ex1).png)<br>
![example2](img/Format_1_3_1(ex2).png)<br>
![example3](img/Format_1_3_1(ex3).png)

ought to be **parsed by Parser_1_3_1**






***
### Format_1_3_2
#### characteristics:
```
platforms = [
        'Bisnode',
        'CHBeck',
        'ACS',
        'Micromedex',
        'SpringerLink',
        'Naxos',
    ]
```




#### examples:
![example1](img/Format_1_3_2(ex1).png)<br>
![example2](img/Format_1_3_2(ex2).png)<br>
![example3](img/Format_1_3_2(ex3).png)

ought to be **parsed by Parser_1_3_2**





***
### Format_1_5_1
#### characteristics:
```
platforms = [
        'InCites',
    ]
```



#### examples:
![example1](img/Format_1_5_1(ex1).png)<br>

ought to be **parsed by Parser_1_5_1**




***
### Format_1_5_2
#### characteristics:
```
platforms = [
    'Micromedex',
    'Naxos',
]
```


#### examples:
![example1](img/Format_1_5_2(ex1).png)<br>

ought to be **parsed by Parser_1_5_2**
