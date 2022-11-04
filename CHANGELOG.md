# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [7.1.0] - 2022-11-04

### Added
- JsonCounter5SheetReader for reading counter data in JSON format
- static TR, DR, PR, IR_M1 counter 5 Json parsers

### Changed
- csv dialect detection is done using nigiri


## [7.0.0] - 2022-10-21

### Fixed
- should not csv dialect when parsing XLSX files
- Print_ISSN and Online_ISSN were swapped
- exception logic
- dimensions in counter parsers

### Added
- metric_based parser and definition
- aggregators (now it aggregates the same records)
- date_metric_based parser and definition
- sheet_idx condition
- extraction logic (ExtractParams - regex, default, skip_validation, ...)
- function to list all definitions (kind attr distinguishes different definitions)
- mandatory function dimensions to Area (returns dimensions names as list)
- parsing of IR_M1 tabular counter format

### Removed
- DummyParser definition (no longer required since more defintion exists now)
- some unused fields from Sources

### Changed
- more defintion refactored
- code moved to separate files to avoid cicrular deps
- fixed definition renamed to non_counter.date_based
- counter parsers and definition redone
- reduced some repetitive parts of the code
- instead of format_name data_format field (of DataFormat class) is used
- parsers names unified


## [6.0.0] - 2022-10-03

### Fixed
- Or condition behavior

### Added
- counter defintions for dynamic parsers
- overrided for counter definitions (heuristics, metric column, ...)
- support for TR_B1 files

### Changed
- definitions refactored
- every definition needs to have name specified (currently "fixed" or "counter")


## [5.4.2] - 2022-09-22

### Fixed
- Title with None no longer crashes the validation


## [5.4.1] - 2022-09-19

### Fixed
- use importlib.metadata instead of pkg_resources


## [5.4.0] - 2022-09-19

### Added
- add function which converts error to dict


## [5.3.0] - 2022-08-08

### Changed
- use latest celus-nigiri (1.1.1)


## [5.2.0] - 2022-07-22

### Changed
- use CounterRecord from celus_nigiri


## [5.1.0] - 2022-06-22

### Added
- organization to CounterRecord


## [5.0.0] - 2022-06-17

### Fixed
- make error comparion more error prune

### Changed
- treat plaform as ordinary dimension

### Added
- caching of reader lines
- implemented dimensions and metric aliases
- version of format into definition
- more tests for dynamic parsers


## [4.0.2] - 2022-06-10

### Fixed
- split NoParserFound error to NoParserMatchesHeuristics and NoParserForPlatformFound
- improve error display
- fix Value and SheetAttr to TableException conversion


## [4.0.1] - 2022-06-08

### Fixed
- Fill title IDs with empty string when they are empty (they were skipped before)


## [4.0.0] - 2022-05-24

### Changed
- Parser should contain a mandatory field `format_name` (unique id of output format)
- added `dynamic_parsers` attribute to `eat()` function

### Added
- Serializable definitions
- Serialization of Coord and CoordRange
- DynamicParser which is able to generate a parser based of a definition
- Value, SheetAttr classes added
- Value, SheetAttr, Coord became feeders for output data (like CoordRange)
- nibbler-eat() can use dynamic parser by passing parser defintion in a file (`-D`)


## [3.0.0] - 2022-05-24

### Changed
- `eat()` returns list of Poop or Exception (not raised)

### Added
- C5 format parssing (csv/tsv) - DR, PR, TR
- nibbler-eat binary args extended


## [2.2.0] - 2022-04-28

### Changed
- match parsers based on regex not just using startswith


## [2.1.0] - 2022-04-27

### Changed
- allow empty values in Dimensions data
- make title IDs and dimensions compatible with celus
- set default CounterRecord.title_ids and CounterRecord.dimension_data to empty dict
- function `eat()` now accepts both str and pathlib.Path as path argument


## [2.0.1] - 2022-04-25

### Fixed
- "Print ISSN" is ISSN and "Online ISSN" is EISSN in counter 4 formats


## [2.0.0] - 2022-04-25

### Added
- sheet_idx property added to Poop class (sheet index from were the Poop comes from)
- get_months() function added to Poop class
- C4 format parsing (csv/tsv) - BR3, DB1, DB2, PR1, JR1, JR1a, JR1GOA, JR2, MR1

### Changed
- CounterRecord.platform removed (should be used as a part of `dimensions_data`)
- C4 parsing is done in more dynamic way (no hardcoded lines nor columns)
- eat() API extended - new options added `parsers`, `check_platform`, `use_heuristics`
- eat() will return list which will contain None if nibbler is unable to parse the sheet

### Removed
- CounterRecord.platform removed (should be used as a part of `dimensions_data`)


## [1.1.0] - 2022-02-04

### Added
* simple BR2 format parsing

### Changed
* added more platforms for BR1 format

## [1.0.0] - 2022-02-03

### Added
- use python entry_points to add parsers
- more complex conditions for heuristics
- support for reading multiple tables from a single sheet
- simple counter BR1 data parser
- parsing title_ids
- parsing dimensions
- use chardet to detect encodings

### Changed
- readers update - process files in streaming mode
- new eat and poop API introduced
- parsers reworked (new coords range abstraction)
- non-free parts removed
- relicensing nibbler to MIT
- `nibbler-eat` doesn't display debug log messages by default

### Removed
- old API
- old parsing functinality

## [0.3.0] - 2021-11-11

### Added
- `nibbler-eat` returns list of supported platforms
- two new parsers added
- `ignore_metrics` - option to ignore specific metrics
- parsing of .xlsx files
- multiple sheets handling within one document
- date parsing improvements

### Changed
- `find_parser_and_parse` - return list of list of records now
- using extra python classes insted of dict in parsing

### Removed
- `find_new_metrics` - no longer used

## [0.2.0] - 2021-08-26

### Added
- `get_supported_platforms` api call added

### Changed
- `find_parser_and_parse` function returns only records

## [0.1.1] - 2021-08-20

### Fixed
- fixed parsing of couple of test files
- make sure that all test files are used for parsing

## [0.1.0] - 2021-08-13

### Added
- parsing data to records
- finding parser based on the file and platform
- data validations
- nibbler binary implemented
- documentation of the formats added
