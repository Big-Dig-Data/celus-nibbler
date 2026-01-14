# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [13.0.0] - 2026-01-14

### Added
- extend ExtractParams with skip_condition and value_overrides

### Changed
- update dependencies
- drop support for older versions of python (oldest supported version is 3.12)


## [12.4.1] - 2026-01-07

### Fixed
- records were sharing dimension_data, now they are copied


## [12.4.0] - 2025-12-03

### Added
- IR_M1 parsers for C51


## [12.3.0] - 2025-11-18

### Changed
- Using uv instead of poetry

### Fixed
- poop.parser.name should contain a proper parser name


## [12.2.3] - 2025-09-24

### Fixed
- parsers_info should not contain None values


## [12.2.2] - 2025-09-24

### Fixed
- display TableException with absolute coords
- allow 5.0 as Release for C5 reports
- unite extras field processing for empty values


## [12.2.1] - 2025-06-26

### Fixed
- make nigiri dependency more general


## [12.2.0] - 2025-05-21

### Added
- squash multiple spaces from titles, dimensions texts, organizations and metrics


## [12.1.0] - 2025-05-16

### Added
- hours_to_minutes special extractor


## [12.0.0] - 2025-04-03

### Added
- configurable dimension validators
- YOP validator
- COUNTER 5.1 parser definitions

### Changed
- removed deprecated date_based, metic_based and date_metric_based parsers

### Fixed
- content caching and dynamic areas extraction
- use older version of poetry for python 3.8 in CI


## [11.5.3] - 2024-12-12

### Fixed
- handling of empty C5 and C5.1 data files


## [11.5.2] - 2024-12-03

### Fixed
- crashing get_months updated


## [11.5.1] - 2024-11-28

### Fixed
- Removed Section_Type from COUNTER 5.1 TR


## [11.5.0] - 2024-11-25

### Added
- test for max_id during metrics extraction
- make it possible to extract year/month from row and other month/year from header
- make C4 heuristics more tolerant
- parsing of CONTER 5.1

### Fixed
- branch with undefined variable


## [11.4.0] - 2024-07-25

### Added
- different offsets bases for Coord ("start", "parser", "area")
- validator to force aligned dates
- validator to convert HH:MM:SS to seconds


### Fixed
- handle situation when there is no date in output CounterRecord more gracefully
- dynamic area offset calculations
- normalize date when it is parsed using date_pattern


## [11.3.0] - 2024-07-25

### Added
- dynamic areas
- fallbacks for sources


## [11.2.1] - 2024-06-11

### Changed
- pydantic updated to 2.7.3


## [11.2.0] - 2024-06-11

### Added
- parse item_authors and item_publication_date
- make sure that item_* are included in all parsers


## [11.1.2] - 2024-05-10

### Changed
- bump nigiri to 2.2.0


## [11.1.1] - 2024-05-02

### Fixed
- dimensions_to_skip wasn't actually skipping records


## [11.1.0] - 2024-04-16

### Added
- wrapping of xlrd error
- add Parent_Data_Type as a dimension for IR reports

### Changed
- bump nigiri to 2.1.0


## [11.0.0] - 2024-03-25

### Fixed
- counter-like output of nibbler-eat

### Added
- limit the number of lines parsed via ExtractParams.max_idx
- display better exception when Exclude_Monthly_Details=True is used for C5 reports
- parser for C5 IR reports

### Changed
- pydantic updated to 2.6.4
- make conter-like output look more like counter data
- use ruff instead of black
- become compatible with nigiri >=2.0.0
- in item parsing of IR_M1 reports use CounterRecord.item instead of CounterRecord.title


## [10.1.3] - 2023-11-13

### Changed
- bump nigiri to 1.3.3


## [10.1.2] - 2023-11-07

### Changed
- bump nigiri to 1.3.1
- use nltk instead of jellyfish


## [10.1.1] - 2023-10-26

### Fixed
- Don't close opened files in DictReader


## [10.1.0] - 2023-10-20

### Fixed
- handle a situation when no date is provided in date validators

### Added
- store idx into exception when RecordError occurs
- reading xls files (optional dependency)
- implement DictReader abstraction over SheetReader
- make it possible to pass opened files to all Readers

### Changed
- improved counter header detection


## [10.0.0] - 2023-10-03

### Fixed
- value validator should strip content before number is parsed
- properly terminate when no metric is present for counter formats

### Added
- isbn13 and isbn10 only validators
- special value extractor MinutesToSeconds
- make it possible to override value validators per metric
- add for C5 Json parsers parser_info

### Changed
- updated pydantic to 2.3.0
- small optimization of pydantic models loading
- using pydantic dataclasses for validators
- nibbler-eat: make platfrom optional param


## [9.1.0] - 2023-08-25

### Added
- extend NoParserMatchesHeuristics with information obtained from parsers (only C4 and C5)
- extract data and put it to Poops.extras (currently only C4 and C5 headers)


## [9.0.1] - 2023-08-14

### Fixed
- performace regression introduced with PoopStats


## [9.0.0] - 2023-08-09

### Fixed
- title detection for counter format parsing was fixed

### Added
- PoopStat.organizations updated to provide more info per orgnization
- Organization is extracted from C5 reports according to the standard

### Changed
- using newer version of pydantic 2.1 with significant performance boost

### Removed
- deprecated `get_metrics_dimensions_title_ids_months` function was removed


## [8.3.4] - 2023-07-26

### Fixed
- pydantic error when reusing date_pattern attribute of DateSource



## [8.3.3] - 2023-07-20

### Fixed
- CI linters


## [8.3.2] - 2023-07-20

### Fixed
- make allowed_metrics working in data header processing


## [8.3.1] - 2023-07-19

### Fixed
- aliases in data header stopped working


## [8.3.0] - 2023-07-19

### Added
- fallback when processing title_ids
- generic parser and parser definition
- allow to process log-like files without values
- new action to data header parsing
- new `available_metrics` and `on_metric_check_failed` parser attributes

### Fixed
- wrong exception handling during data header processing


## [8.2.1] - 2023-06-05

### Fixed
- bump nigiri to 1.3.1


## [8.2.0] - 2023-05-30

### Added
- Poop.area_counter to count the number of records from each area
- Poop.records_with_stats function which fills Poop.current_stats while processing the records


## [8.1.0] - 2023-05-24

### Added
- IsDateCondition can be used to check that field contains a date (using validators.Date)
- Conditions for areas - this enables to dynamically detect areas position based on provided condition

### Changed
- removed `parse_date` function to unite date parsing logic
- use `data_headers` for celus format (so the parsing behaves more similar to regular parsers)

### Fixed
- removed double caching in `CounterHeaderArea.header_row`


## [8.0.0] - 2023-04-28

### Added
- on_validation_error for ExtractParams

### Changed
- the way how errors are handled


## [7.14.2] - 2023-04-25

### Fixed
- use calculate_dimensions in xlsx parsing properly (even for unsized worksheets)


## [7.14.1] - 2023-04-25

### Fixed
- remedy for parsing of messed up xlsx files


## [7.14.0] - 2023-04-24

### Added
- prefix and suffix to extract parameters
- configurable row offsets for parsers


## [7.13.0] - 2023-04-19

### Added
- allow to choose between US 1/24/2022 and EU 24/1/2022 date format
- allow to manually pick date format
- compose date from two cells (month cell and year cell)


## [7.12.0] - 2023-03-30

### Added
- allow "Metric Type" as "Metric_Type" alias

### Fixed
- python 3.11 compatibility issues


## [7.11.1] - 2023-03-24

### Fixed
- explicitly closing SheetReader
- don't link SheetReader to Source


## [7.11.0] - 2023-03-24

### Added
- add function to merge PoopStats


## [7.10.1] - 2023-03-20

### Fixed
- typo in SpecialExtraction.COMMA_SEPARATED_NUMBER
- use tuple in record hash calculation


## [7.10.0] - 2023-03-20

### Added
- allow to parse numbers with comma separated digits (e.g. '1,123,456')
- added `Poop.get_stats` to gather various info regarding output data

### Changed
- make function `get_metrics_dimensions_title_ids_months` deprecated

### Fixed
- don't stop when an empty Platform dimension occurs during counter data parsing
- setting nested data while merging CounterRecord


## [7.9.0] - 2023-03-06

### Added
- case insensitive metric_to_skip, titles_to_skip and dimesions_to_skip
- skip empty lines while parsing tabular counter reports
- add offset and limit to Poop.records()
- extend eat() function with same_check_size attribute to check for the same records
- extend Poop with data_format attribute
- allow to use CoordRange in conditions
- make sure that the line lenght is not decreasing during XLSX -> CSV conversion

### Fixed
- don't check for the name of the report in C5 reports


## [7.8.1] - 2023-02-16

### Fixed
- bump nigiri to 1.3.0


## [7.8.0] - 2023-02-07

### Fixed
- don't convert CounterReport to csv before passing it to debug logger

### Added
- flag `--show-summary` to nibbler-eat
- flag `--no-output` to  nobbler-eat


### Changed
- make counter sources cached
- avoid seeking to the first postion in a file when moving window of CsvReader forward
- optimize extract function - reduce the number of created Coords


## [7.7.6] - 2023-02-06

### Fixed
- increase max header row size to 1000


## [7.7.5] - 2023-02-03

### Fixed
- counter 5 parser is not ignoring extra dimensions


## [7.7.4] - 2023-02-01

### Fixed
- cache default value Validator generation to speed up extraction


## [7.7.3] - 2023-02-01

### Fixed
- add 'Printed_ISSN' and 'Printed ISSN' aliases for ISSN
- make dimension detection case insensitive for counter data
- remove constraint that title can't be a number


## [7.7.2] - 2023-01-30

### Fixed
- don't store empty values to title_ids
- add missing `URI` to title_ids
- less strict validators (isbn, issn, doi)
- consider empty values as zeros in Tabular counter reports


## [7.7.1] - 2023-01-27

### Fixed
- strip titles during parsing


## [7.7.0] - 2023-01-25

### Added
- allow to parse JSON celus format with empty header


## [7.6.2] - 2023-01-10

### Fix
- fix extractor caching between sheets


## [7.6.1] - 2023-01-08

### Fix
- wrong naming in DataHeaders rules


## [7.6.0] - 2023-01-06

### Changed
- searching for data columns is more customizable using a set of rules


## [7.5.0] - 2023-01-05

### Added
- ability to skip or stop while searching for data columns


## [7.4.0] - 2022-12-19

### Fixed
- proper parsing of dates in `Dec-21` format

### Added
- `nibbler-eat` is able to generate celus-format output

### Changed
- parsing performance optimizations


## [7.3.1] - 2022-12-05

### Changed
- nigiri version bumped


## [7.3.0] - 2022-11-25

### Added
- value_extract_params option added to CelusFormat parser and definition


## [7.2.1] - 2022-11-11

### Fixed
- use default_metric instead of override_metric in celus format


## [7.2.0] - 2022-11-10

### Added
- added dynamic parser for non-counter data in celus format


## [7.1.1] - 2022-11-08

### Fixed
- use proper csv dialect detectio when using nigiri


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
