"""Module interface.py"""
import logging

import cudf
import dask
import pandas as pd

import src.algorithms.data
import src.algorithms.persist
import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix


class Interface:
    """
    The interface to quantiles calculations.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

        # Quantile points
        self.__q_points = {0.10: 'l_whisker', 0.25: 'l_quartile', 0.50: 'median', 0.75: 'u_quartile', 0.90: 'u_whisker'}

    def __quantiles(self, data: cudf.DataFrame, quantile: float) -> cudf.DataFrame:
        """

        :param data: A data frame consisting of fields ['date', 'measure'] <b>only</b>.<br>
        :param quantile: A quantile point; 0 &le; quantile point &le; 1.<br>
        :return:
        """

        part = data.groupby(by='date', as_index=True, axis=0).quantile(q=quantile)

        return part.rename(columns={'measure': self.__q_points[quantile]})

    @dask.delayed
    def __get_metrics(self, data: cudf.DataFrame) -> cudf.DataFrame:
        """

        :param data: A data frame consisting of fields ['date', 'measure'] <b>only</b>.<br>
        :return:
        """

        sections = [self.__quantiles(data=data, quantile=quantile) for quantile in self.__q_points.keys()]
        instances = cudf.concat(sections, axis=1, ignore_index=False)

        return instances

    def exc(self, partitions: list[pr.Partitions], reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        # Delayed tasks
        __data = dask.delayed(src.algorithms.data.Data(
            service=self.__service, s3_parameters=self.__s3_parameters, arguments=self.__arguments).exc)
        __persist = dask.delayed(src.algorithms.persist.Persist(reference=reference).exc)

        computations = []
        for partition in partitions[:3]:
            data = __data(partition=partition)
            metrics = self.__get_metrics(data=data)
            message = __persist(metrics=metrics, partition=partition)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads')[0]

        logging.info(messages)
