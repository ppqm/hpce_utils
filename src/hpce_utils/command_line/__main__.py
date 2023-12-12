import logging

from hpce_utils import __version__

logger = logging.getLogger(__name__)


def main(args=None):
    import argparse

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action="version", version=__version__)
    args = parser.parse_args(args)


if __name__ == "__main__":
    main()
