# from pyspark import SparkContext
# from pyspark.sql import SparkSession

from core.pipeline import PipelineDefinition, Pipeline

with open('/Users/kb/PycharmProjects/pyspark-etl/example-pipeline.yaml') as pipeline_yaml_stream:
    pld = PipelineDefinition()
    pld.build_from_yaml(yaml_file_stream=pipeline_yaml_stream)
    print(pld)
    pipeline = Pipeline(definition=pld)
    pipeline.run()