from argparse import ArgumentParser

from sparklanes import build_lane_from_yaml


def main():
    args = parse_args()
    build_lane_from_yaml(args['lane']).run()


def parse_args():
    parser = ArgumentParser()

    parser.add_argument('-l', '--lane',
                        help='Relative or absolute path to the lane definition YAML file',
                        type=str,
                        required=True)

    return parser.parse_args().__dict__


if __name__ == '__main__':
    main()
