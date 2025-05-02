import logging

import cudf
import cudf.core.groupby as ccg
import numpy as np
import pandas as pd

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix
import dask


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

        self.__q = {0.25: 'l_quartile', 0.50: 'median'}

    def __q_tiles(self, blob: pd.DataFrame, q: float):

        part = blob.groupby(by='date', as_index=True, axis=0).quantile(q=q)
        return part.rename(columns={'measure': self.__q[q]})

    def __experiment(self, partition: pr.Partitions):
        """

        :param partition:
        :return:
        """

        listings = self.__pre.objects(prefix=partition.prefix.rstrip('/'))
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]

        blocks = [cudf.read_csv(filepath_or_buffer=key, header=0, usecols=['timestamp', 'ts_id', 'measure']) for key in keys]
        block = cudf.concat(blocks)
        block['datestr'] = cudf.to_datetime(block['timestamp'], unit='ms')
        block['date'] = block['datestr'].dt.strftime('%Y-%m-%d')

        sc = dask.delayed(self.__q_tiles)

        computations = []
        for q in [0.25, 0.50]:
            metrics = sc(block[['date', 'measure']], q)
            computations.append(metrics)
        calc = dask.compute(computations)[0]
        calculations = cudf.concat(calc, axis=1, ignore_index=False)
        logging.info(calc)
        logging.info(calculations)


    def exc(self, partitions: list[pr.Partitions]):
        """

        :return:
        """

        for partition in partitions[:2]:
            logging.info(partition.uri)
            self.__experiment(partition=partition)
