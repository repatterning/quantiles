import logging

import cudf

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix
import dask.dataframe as ddf


class Interface:

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service:
        :param s3_parameters:
        :param arguments:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

        # An instance for interacting with objects within an Amazon S3 prefix
        self.__bucket_name = self.__s3_parameters._asdict()[arguments['s3']['p_bucket']]
        self.__pre = src.s3.prefix.Prefix(
            service=self.__service,
            bucket_name=self.__bucket_name)

        # Logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d\n',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __experiment(self, partition: pr.Partitions):
        """

        :param partition:
        :return:
        """

        listings = self.__pre.objects(prefix=partition.prefix.rstrip('/'))
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]

        for key in keys:

            part = cudf.read_csv(filepath_or_buffer=key, header=0, usecols=['timestamp', 'ts_id', 'measure'])
            part['datestr'] = cudf.to_datetime(part['timestamp'], unit='ms')
            part['date'] = part['datestr'].dt.strftime('%Y-%m-%d')
            logging.info(part.head())

    def exc(self, partitions: list[pr.Partitions]):
        """

        :return:
        """

        for partition in partitions[:2]:
            logging.info(partition.uri)
            self.__experiment(partition=partition)
