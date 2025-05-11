"""Module persist.py"""
import json
import os

import pandas as pd

import config
import src.elements.partitions as pr
import src.functions.objects


class Persist:
    """
    Persist
    """

    def __init__(self, reference: pd.DataFrame):
        """

        :param reference: A reference of gauges, and their attributes.
        """

        self.__reference = reference

        self.__configurations = config.Config()
        self.__objects = src.functions.objects.Objects()

    def __get_nodes(self, data: pd.DataFrame, ts_id: int) -> dict:
        """

        :param data: Quantiles
        :param ts_id: A time series identification code
        :return:
        """

        attributes: pd.Series = self.__reference.loc[self.__reference['ts_id'] == ts_id, :].squeeze()

        string = data.to_json(orient='split')
        nodes = json.loads(string)
        nodes.update(attributes.to_dict())

        return nodes

    def exc(self, quantiles: pd.DataFrame, extrema: pd.DataFrame, partition: pr.Partitions):
        """

        :param quantiles: The quantiles
        :param extrema: The extrema
        :param partition: Refer to src/elements/partitions.py
        :return:
        """

        metrics = quantiles.merge(extrema, how='left', on='datestr')
        metrics.sort_values(by='datestr', ascending=True, ignore_index=True, inplace=True)

        # The nodes
        nodes = self.__get_nodes(data=metrics, ts_id=partition.ts_id)

        # Write
        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{partition.ts_id}.json'))

        return message
