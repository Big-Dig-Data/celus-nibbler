# Celus Nibbler Documentation

## Requirements

- [] there should not be a parser class which serves to two tables which both are having different dimensions.
- each type of non-counter report has its own parser

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

## Exceptions
- if wrong parser is used for a report, it tells in what place validation failed and how it failed.

## Tests
- `test_findparser()` - going through all tables in directory `celus-nibbler/tests/data/csv/<parser_name>` and tests whether `findparser()` assigns for each table a parser which name corresponds with directory `<parser_name>` in which the table is located.
- `test_findparser_and_parse()` - goes through each item in `testing_data.data` and checks wheter `findparser_and_parse()` finds the exact parser and parses the exact counter-report as mentioned in `testing_data.data`.


## Formats

each parser recognizes corresponding table to parse by matching its `heuristics`, `metric_title` and table platform is in parsers `platforms`. See class attributes of each parser in `celus_nibbler/parsers/format_....py` respectively.


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

```
table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'Metric'},
        ],
        'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
        'months': {
            'direction': 'in cols',
            'start_at': {
                'row': 0,
                'col': 1,
                'month': 'january',
            },
        },
    }
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

```
table_map = {
    'heuristics': [
        {'row': 1, 'col': 0, 'content': ''},
    ],
    'metric_title': {'row': 1, 'col': 0, 'content': ''},
    'months': {
        'direction': 'in cols',
        'start_at': {
            'row': 1,
            'col': 1,
            'month': 'january',
        },
    },
}
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

```
table_map = {
        'heuristics': [
            {'row': 0, 'col': 0, 'content': 'Title'},
            {'row': 0, 'col': 1, 'content': 'Metric'},
        ],
        'metric_title': {'row': 0, 'col': 1, 'content': 'Metric'},
        'months': {
            'direction': 'in cols',
            'start_at': {
                'row': 0,
                'col': 2,
                'month': 'january',
            },
        },
    }
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

```
table_map = {
    'heuristics': [
        {'row': 0, 'col': 0, 'content': 'Metric'},
        {'row': 0, 'col': 1, 'content': 'Title'},
    ],
    'metric_title': {'row': 0, 'col': 0, 'content': 'Metric'},
    'months': {
        'direction': 'in cols',
        'start_at': {
            'row': 0,
            'col': 2,
            'month': 'january',
        },
    },
}
```


#### examples:
![example1](img/Format_1_5_2(ex1).png)<br>

ought to be **parsed by Parser_1_5_2**
