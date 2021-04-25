import logging
import logging.config
import pathlib
import sys

from celus_nibbler import findparser_and_parse
from celus_nibbler.logging_config import DEFAULT_LOGGING  # noqa


def main():
    logging.basicConfig(level=logging.DEBUG)
    # logging.config.dictConfig(DEFAULT_LOGGING)

    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    if len(sys.argv) < 3:
        print("Some arguemnts are missing")
        print()
        print("usage:")
        print(f"    {pathlib.Path(sys.argv[0]).name} platform file [file..]")
        sys.exit(1)

    platform = sys.argv[1]

    for file in sys.argv[2:]:
        if parsed := findparser_and_parse(pathlib.Path(file), platform):
            (_, _, records) = parsed
            for record in records:
                print(record)


if __name__ == "__main__":
    main()
