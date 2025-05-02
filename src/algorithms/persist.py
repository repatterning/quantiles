import logging
import cudf
import json
import pandas as pd

import src.elements.partitions as pr


class Persist:

    def __init__(self, reference: pd.DataFrame):
        """

        :param reference:
        """

        self.__reference = reference

    def __get_dictionary(self, data: pd.DataFrame, ts_id: int):
        """

        :param data:
        :param ts_id:
        :return:
        """

        attributes: pd.Series = self.__reference.loc[self.__reference['ts_id'] == ts_id, :].squeeze()

        string = data.to_json(orient='split')
        dictionary = json.loads(string)
        dictionary.update(attributes.to_dict())


    def exc(self, metrics: cudf.DataFrame, partition: pr.Partitions) -> str:
        """

        :param metrics:
        :param partition:
        :return:
        """

        # To pandas DataFrame format
        data = metrics.to_pandas().reset_index(drop=False)

        # Ascertain date order
        data.sort_values(by='date', ascending=True, ignore_index=True, inplace=True)

        # The dictionary
        self.__get_dictionary(data=data, ts_id=partition.ts_id)

        return 'in progress'
