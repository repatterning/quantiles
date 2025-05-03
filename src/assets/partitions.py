"""Module partitions.py"""
import numpy as np
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame, arguments: dict):
        """

        :param data:
        :param arguments:
        """

        self.__data = data
        self.__arguments = arguments

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        codes = np.array(self.__arguments.get('excerpt'))
        codes = np.unique(codes)

        if self.__arguments.get('reacquire') | (codes.size == 0):
            return self.__data

        frame = self.__data.copy()[self.__data['ts_id'].isin(codes), :]

        return frame
