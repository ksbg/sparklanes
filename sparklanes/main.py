from argparse import ArgumentParser
from io import open

from sparklanes.framework.pipeline import PipelineDefinition, Pipeline


def main():
    args = parse_args()

    with open(args['lane'], 'rb') as pipeline_yaml_stream:
        pld = PipelineDefinition()
        pld.build_from_yaml(yaml_file_stream=pipeline_yaml_stream)
        pipeline = Pipeline(definition=pld)
        pipeline.logger.info(str(pld))
        pipeline.run()


def parse_args():
    parser = ArgumentParser()

    parser.add_argument('-p', '--lane',
                        help='Relative or absolute path to the lane definition YAML file',
                        type=str,
                        required=True)

    return parser.parse_args().__dict__


if __name__ == '__main__':
    main()
