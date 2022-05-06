import argparse
import logging
import logging.config
import pathlib
import sys

import pkg_resources
from unidecode import unidecode

from celus_nibbler import eat, get_supported_platforms_count
from celus_nibbler.parsers import available_parsers


def main():
    dist = pkg_resources.get_distribution("celus-nibbler")
    version = dist.version if dist else "?"

    platforms_count = "\n".join(
        [f"  {platform}({count})" for platform, count in get_supported_platforms_count()]
    )
    parsers_list = "\n".join([f"  {e}" for e in available_parsers()])

    description = f"""Supported platforms
{platforms_count}

Available Parsers:
{parsers_list}
"""
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
    parser.add_argument("platform")
    parser.add_argument("files", nargs='+')
    options = parser.parse_args()

    # init logging
    logging.basicConfig(level=logging.DEBUG if options.debug else logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    platform = unidecode(options.platform)

    for file in options.files:
        if poops := eat(
            pathlib.Path(file),
            platform,
            parsers=options.parser or None,
            check_platform=not options.ignore_platform,
            use_heuristics=not options.skip_heuristics,
        ):
            for poop in poops:
                if not (poop):
                    print("Failed to pick parser")
                    sys.exit(1)
                for record in poop.records():
                    print(",".join((f'"{e}"' if e else "") for e in record.serialize()))


if __name__ == "__main__":
    main()
