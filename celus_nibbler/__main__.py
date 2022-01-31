import logging
import logging.config
import pathlib
import sys

from unidecode import unidecode

from celus_nibbler import eat, get_supported_platforms_count


def main():
    logging.basicConfig(level=logging.DEBUG)

    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    if len(sys.argv) < 3:
        print("Some arguemnts are missing")
        print()
        print("usage:")
        print(f"    {pathlib.Path(sys.argv[0]).name} platform file [file..]")
        print()
        print("Supported platforms:")
        for platform, count in get_supported_platforms_count():
            print(f"  {platform}({count})")
        sys.exit(1)

    platform = unidecode(sys.argv[1])

    for file in sys.argv[2:]:
        if poops := eat(pathlib.Path(file), platform):
            for poop in poops:
                for record in poop.records():
                    print(",".join((f'"{e}"' if e else "") for e in record.serialize()))


if __name__ == "__main__":
    main()
