import logging

import cudf
import dask
import pandas as pd

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix
import src.algorithms.persist
import src.algorithms.data


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

        # Quantile points
        self.__q_points = {0.10: 'l_whisker', 0.25: 'l_quartile', 0.50: 'median', 0.75: 'u_quartile', 0.90: 'u_whisker'}

    def __quantiles(self, data: cudf.DataFrame, quantile: float) -> cudf.DataFrame:
        """

        :param data:
        :param quantile:
        :return:
        """

        part = data.groupby(by='date', as_index=True, axis=0).quantile(q=quantile)

        return part.rename(columns={'measure': self.__q_points[quantile]})

    @dask.delayed
    def __get_metrics(self, data: cudf.DataFrame) -> cudf.DataFrame:
        """

        :param data:
        :return:
        """

        sections = [self.__quantiles(data=data, quantile=quantile) for quantile in self.__q_points.keys()]
        instances = cudf.concat(sections, axis=1, ignore_index=False)

        return instances

    def exc(self, partitions: list[pr.Partitions]):
        """

        :return:
        """

        # Delayed tasks
        __data = dask.delayed(src.algorithms.data.Data(
            service=self.__service, s3_parameters=self.__s3_parameters, arguments=self.__arguments).exc)
        __persist = dask.delayed(src.algorithms.persist.Persist().exc)

        computations = []
        for partition in partitions[:3]:
            data = __data(partition=partition)
            metrics = self.__get_metrics(data=data)
            message = __persist(metrics=metrics)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads')[0]

        logging.info(messages)
