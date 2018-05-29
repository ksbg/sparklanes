from argparse import ArgumentParser
from sparklanes import build_lane_from_yaml
if __name__ == '__main__':
    parser = ArgumentParser().add_argument('-l', '--lane', required=True)
    lane = parser.parse_args().__dict__['lane']
    build_lane_from_yaml(lane).run()
