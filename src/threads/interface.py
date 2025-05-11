"""Module interface.py"""
import logging

import dask
import pandas as pd

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.threads.data
import src.threads.extrema
import src.threads.persist
import src.threads.quantiles


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

    def exc(self, partitions: list[pr.Partitions], reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        # Delayed tasks
        _data = dask.delayed(src.threads.data.Data(
            service=self.__service, s3_parameters=self.__s3_parameters, arguments=self.__arguments).exc)
        _extrema = dask.delayed(src.threads.extrema.Extrema().exc)
        _persist = dask.delayed(src.threads.persist.Persist(reference=reference).exc)
        _quantiles = dask.delayed(src.threads.quantiles.Quantile().exc)

        # Compute
        computations = []
        for partition in partitions[:2]:
            data = _data(partition=partition)
            quantiles = _quantiles(data=data)
            extrema = _extrema(data=data)
            message = _persist(quantiles=quantiles, extrema=extrema, partition=partition)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads')[0]

        logging.info(messages)
