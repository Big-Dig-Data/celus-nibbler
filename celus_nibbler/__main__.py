import argparse
import csv
import json
import logging
import logging.config
import pathlib
import sys
import typing
from copy import deepcopy
from datetime import date
from importlib.metadata import distribution

from celus_nigiri import CounterRecord
from unidecode import unidecode

from celus_nibbler import Poop, eat
from celus_nibbler.aggregator import CounterOrdering
from celus_nibbler.definitions import Definition
from celus_nibbler.parsers import available_parsers, get_supported_platforms_count
from celus_nibbler.parsers.dynamic import gen_parser


def read_definition(path: str) -> Definition:
    with pathlib.Path(path).open() as f:
        definition_data = json.load(f)
    return Definition.parse(definition_data)


def gen_description(
    platforms_count_list: typing.List[typing.Tuple[str, int]], parsers: typing.List[str]
) -> str:
    platforms_count = "\n".join(
        [f"  {platform}({count})" for platform, count in platforms_count_list]
    )
    parsers_list = "\n".join([f"  {e}" for e in parsers])

    return f"""Supported platforms
{platforms_count}

Available Parsers:
{parsers_list}
"""


def write_batch(
    writer,
    records: typing.List[CounterRecord],
    dimensions: typing.List[str],
    title_ids: typing.List[str],
    months: typing.List[date],
):
    months_dict = {e: "" for e in months}
    for r in records:
        months_dict[r.start] = r.value
    writer.writerow(
        [r.title, r.organization]
        + [r.title_ids[ti] for ti in title_ids]
        + [r.dimension_data[d] for d in dimensions]
        + [r.metric]
        + [months_dict[k] for k in sorted(months)]
    )


def gen_argument_parser(
    platforms_count_list: typing.List[typing.Tuple[str, int]], parsers: typing.List[str]
) -> argparse.ArgumentParser:
    dist = distribution("celus-nibbler")
    version = dist.version if dist else "?"
    description = gen_description(platforms_count_list, parsers)
    parser = argparse.ArgumentParser(
        prog="nibbler-eat",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )
    parser.add_argument("-d", "--debug", dest="debug", action="store_true", default=False)
    parser.add_argument("--version", action="version", version=version)
    parser.add_argument(
        "-i", "--ignore-platform", dest="ignore_platform", action="store_true", default=False
    )
    parser.add_argument(
        "-s",
        "--skip-heuristics",
        dest="skip_heuristics",
        action="store_true",
        default=False,
        help="skip heuristic checks",
    )
    parser.add_argument(
        "-p",
        "--parser",
        action="append",
        default=[],
        help="limit parsers which can be used",
    )
    parser.add_argument(
        "-D",
        "--definition",
        action="append",
        default=[],
        help="Reads a definition of a dynamic parser from a file",
        type=read_definition,
    )
    parser.add_argument(
        "-c",
        "--counter-like-output",
        action="store_true",
        default=False,
        help="Display processed data in counter like format",
    )
    parser.add_argument("platform")
    parser.add_argument("file", nargs='*')
    return parser


def main():
    parser = gen_argument_parser(get_supported_platforms_count(), available_parsers())
    options = parser.parse_args()

    # init logging
    logging.basicConfig(level=logging.DEBUG if options.debug else logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    platform = unidecode(options.platform)
    dynamic_parsers = [gen_parser(e) for e in options.definition]

    if not options.file:
        # Just print help if not parsers specified
        parsers = options.parser
        # Enrich with dynamic parsers
        parser = gen_argument_parser(
            get_supported_platforms_count(parsers, dynamic_parsers),
            available_parsers(dynamic_parsers),
        )
        parser.print_help()
        return

    for file in options.file:
        if poops := eat(
            pathlib.Path(file),
            platform,
            parsers=options.parser or None,
            check_platform=not options.ignore_platform,
            use_heuristics=not options.skip_heuristics,
            dynamic_parsers=dynamic_parsers,
        ):
            for idx, poop in enumerate(poops):
                if not isinstance(poop, Poop):
                    print(f"Failed to pick parser for sheet {idx}", file=sys.stderr)
                    continue

                (
                    metrics,
                    dimensions,
                    title_ids,
                    months,
                ) = poop.get_metrics_dimensions_title_ids_months()

                print(f"Parsing sheet {idx}", file=sys.stderr)
                print(f"Months: {months}", file=sys.stderr)
                print(f"Metrics: {metrics}", file=sys.stderr)
                print(f"Dimensions: {dimensions}", file=sys.stderr)
                print(f"Title ids: {title_ids}", file=sys.stderr)

                if options.counter_like_output:
                    header = (
                        ["title", "organization"]
                        + poop.title_ids
                        + poop.dimensions
                        + ["metric"]
                        + poop.months
                    )
                else:
                    header = [
                        "start",
                        "end",
                        "organization",
                        "title",
                        "metric",
                        "dimensions",
                        "title_ids",
                        "value",
                    ]

                writer = csv.writer(sys.stdout, dialect="unix")
                writer.writerow(header)
                records = (
                    CounterOrdering().aggregate(poop.records())
                    if options.counter_like_output
                    else poop.records()
                )
                last_rec = None
                batch = []
                for record in records:

                    if options.counter_like_output:
                        this_rec = deepcopy(record)
                        this_rec.value = -1
                        this_rec.start = date(2020, 1, 1)
                        this_rec.end = date(2020, 1, 31)

                        if not last_rec or last_rec == this_rec:
                            batch.append(record)
                        elif batch:
                            write_batch(writer, batch, dimensions, title_ids, months)
                            batch = [record]
                        last_rec = this_rec
                    else:
                        writer.writerow(record.as_csv())

                if batch:
                    write_batch(writer, batch, dimensions, title_ids, months)


if __name__ == "__main__":
    main()
