import argparse
import csv
import json
import logging
import logging.config
import pathlib
import sys
import typing
from contextlib import nullcontext
from copy import deepcopy
from datetime import date
from importlib.metadata import distribution

from celus_nigiri import CounterRecord
from unidecode import unidecode

from celus_nibbler import Poop, eat
from celus_nibbler.aggregator import CounterOrdering
from celus_nibbler.definitions import Definition
from celus_nibbler.parsers import available_parsers
from celus_nibbler.parsers.dynamic import gen_parser
from celus_nibbler.utils import profile


def read_definition(path: str) -> Definition:
    with pathlib.Path(path).open() as f:
        definition_data = json.load(f)
    return Definition.parse(definition_data)


def gen_description(parsers: typing.List[str]) -> str:
    parsers_list = "\n".join([f"  {e}" for e in parsers])

    return f"""Available Parsers:
{parsers_list}
"""


def write_batch(
    writer,
    records: typing.List[CounterRecord],
    dimensions: typing.List[str],
    title_ids: typing.List[str],
    item_ids: typing.List[str],
    months: typing.List[str],
):
    months_dict = {e: "" for e in months}
    for r in records:
        months_dict[r.start.strftime("%Y-%m")] = r.value
    writer.writerow(
        [r.organization, r.title]
        + [r.title_ids.get(ti, "") for ti in title_ids]
        + [r.item]
        + [r.item_ids.get(ii, "") for ii in item_ids]
        + [r.dimension_data.get(d, "") for d in dimensions]
        + [r.metric]
        + [months_dict[k] for k in sorted(months)]
    )


def gen_argument_parser(parsers: typing.List[str]) -> argparse.ArgumentParser:
    dist = distribution("celus-nibbler")
    version = dist.version if dist else "?"
    description = gen_description(parsers)
    parser = argparse.ArgumentParser(
        prog="nibbler-eat",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )
    parser.add_argument("-d", "--debug", dest="debug", action="store_true", default=False)
    parser.add_argument("--version", action="version", version=version)
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
        "-S",
        "--show-summary",
        action="store_true",
        default=False,
        help="show per sheet summary",
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
        "-P",
        "--platform",
        default="",
        help="Limit platform that can be used",
    )
    parser.add_argument(
        "-c",
        "--counter-like-output",
        action="store_true",
        default=False,
        help="Display processed data in counter like format",
    )
    parser.add_argument(
        "-N",
        "--no-output",
        action="store_true",
        default=False,
        help="Parses the entire file without producing any data output (useful for benchmarks)",
    )
    parser.add_argument("file", nargs="*")
    parser.add_argument("--profile", dest="profile", action="store_true", default=False)

    return parser


def parse(options, platform, dynamic_parsers):
    for file in options.file:
        if poops := eat(
            pathlib.Path(file),
            platform or "void",
            parsers=options.parser or None,
            check_platform=bool(platform),
            use_heuristics=not options.skip_heuristics,
            dynamic_parsers=dynamic_parsers,
        ):
            for idx, poop in enumerate(poops):
                if not isinstance(poop, Poop):
                    print(f"Failed to pick parser for sheet {idx}", file=sys.stderr)
                    continue

                stat_dict = poop.get_stats().dict()
                if options.show_summary:
                    print(f"Parsing sheet {idx}", file=sys.stderr)
                    print(f"Months: {stat_dict['months']}", file=sys.stderr)
                    print(f"Metrics: {stat_dict['metrics']}", file=sys.stderr)
                    print(f"Dimensions: {stat_dict['dimensions']}", file=sys.stderr)
                    print(f"Title ids: {stat_dict['title_ids']}", file=sys.stderr)
                    print(f"Item ids: {stat_dict['item_ids']}", file=sys.stderr)

                if options.no_output:
                    header = None
                elif options.counter_like_output:
                    header = (
                        ["Institution_Name", "Title"]
                        + list(poop.title_ids)
                        + list(poop.dimensions)
                        + ["Metric_Type"]
                        + list(poop.months)
                    )
                else:
                    header = [
                        "start",
                        "end",
                        "organization",
                        "title",
                        "title_ids",
                        "item",
                        "item_ids",
                        "item_publication_date",
                        "item_authors",
                        "dimensions",
                        "metric",
                        "value",
                    ]

                writer = csv.writer(sys.stdout, dialect="unix")
                if header:
                    writer.writerow(header)
                records = (e[1] for e in poop.records_basic())
                records = (
                    CounterOrdering().aggregate(records) if options.counter_like_output else records
                )
                last_rec = None
                batch = []
                for record in records:
                    if options.no_output:
                        # Just go through the records and suppress any output
                        pass
                    elif options.counter_like_output:
                        this_rec = deepcopy(record)
                        this_rec.value = -1
                        this_rec.start = date(2020, 1, 1)
                        this_rec.end = date(2020, 1, 31)

                        if not last_rec or last_rec == this_rec:
                            batch.append(record)
                        elif batch:
                            write_batch(
                                writer,
                                batch,
                                stat_dict["dimensions"].keys(),
                                stat_dict["title_ids"],
                                stat_dict["item_ids"],
                                stat_dict["months"].keys(),
                            )
                            batch = [record]
                        last_rec = this_rec
                    else:
                        writer.writerow(record.as_csv())

                if batch:
                    write_batch(
                        writer,
                        batch,
                        stat_dict["dimensions"].keys(),
                        stat_dict["title_ids"],
                        stat_dict["item_ids"],
                        stat_dict["months"].keys(),
                    )


def main():
    parser = gen_argument_parser(available_parsers())
    options = parser.parse_args()

    # init logging
    logging.basicConfig(level=logging.DEBUG if options.debug else logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    platform = unidecode(options.platform)
    dynamic_parsers = [gen_parser(e) for e in options.definition]

    if not options.file:
        # Just print help if not parsers specified
        # Enrich with dynamic parsers
        parser = gen_argument_parser(available_parsers(dynamic_parsers))
        parser.print_help()
        return

    context_man = nullcontext if not options.profile else profile

    with context_man():
        parse(options, platform, dynamic_parsers)


if __name__ == "__main__":
    main()
