import argparse
import json
import logging
import logging.config
import pathlib
import sys
import typing
from importlib.metadata import distribution

from unidecode import unidecode

from celus_nibbler import Poop, eat
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
                print(f"Parsing sheet {idx}", file=sys.stderr)
                print(f"Months: {poop.get_months()}", file=sys.stderr)
                for record in poop.records():
                    print(",".join((f'"{e}"' if e else "") for e in record.as_csv()))


if __name__ == "__main__":
    main()
