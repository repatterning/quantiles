"""Module main.py"""
import datetime
import logging
import os
import sys
import boto3
import pyspark.sql

def main():

    logger: logging.Logger = logging.getLogger(__name__)
    logger.info('Starting: %s', datetime.datetime.now().isoformat(timespec='microseconds'))

    logger.info(arguments)

    partitions = src.assets.interface.Interface(
        service=service, s3_parameters=s3_parameters, arguments=arguments).exc()
    logger.info(partitions)


    spark = (pyspark.sql.SparkSession.builder.appName('Quantiles')
             .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.1,org.apache.hadoop:hadoop-client:3.4.1')
             .config('spark.jars.excludes', 'com.google.guava:guava')
             .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.profile.ProfileCredentialsProvider')
             .getOrCreate())

    src.algorithms.interface.Interface(spark=spark, partitions=partitions).exc()

    # Cache
    src.functions.cache.Cache().exc()


if __name__ == '__main__':

    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Logging
    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d\n',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import src.algorithms.interface
    import src.assets.interface
    import src.elements.s3_parameters as s3p
    import src.elements.service as sr
    import src.functions.cache
    import src.preface.interface

    connector: boto3.session.Session
    s3_parameters: s3p.S3Parameters
    service: sr.Service
    arguments: dict
    connector, s3_parameters, service, arguments = src.preface.interface.Interface().exc()

    main()

