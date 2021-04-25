# Celus Nibbler Documentation

- each type of non-counter report has its own parser

## Parsers

- each parser caries:
    - list of Publishers from whom we register particular type of non-counter report.
        - if Publisher not in the list parser wont parse the report.
    - list of metrics we expect from particular type of non-counter report.
        - if metric not in the list parser will parse the report and store the new metric in a list.

- how Nibbler recognizes the correct parser for the non-counter report:
    1. checks if the report is from expected Publisher.
    2. checks for specific heuristic:
      - any values expected at certain places in the report. Each parser has multiple of those checks.
    3. checks for expected title of column with metrics


- each parser checks wheather its getting expected metrics according to the list of metrics it carries.
        - if metric not int the list of metrics the parser will parse the report with message about registering new metric and stores this new metric in its list of expected metrics.

- validations even during parsing

## Exceptions
- if wrong parser is used for a report, it tells in what place validation failed and how it failed.

## Requirements

- [] there should not be a parser class which serves to two tables which both are having different dimensions.
