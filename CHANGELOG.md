# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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
