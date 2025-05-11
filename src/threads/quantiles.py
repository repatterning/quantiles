"""
Module numerics.py
"""
import logging

import pandas as pd
import numpy as np


class Quantile:
    """
    Notes
    -----

    Calculating quantiles
    """

    def __init__(self) -> None:
        """
        Constructor
        """

        # Quantile points
        self.__q = np.array([0.10, 0.25, 0.50, 0.75, 0.90])
        self.__q_points = {0.10: 'l_whisker', 0.25: 'l_quartile', 0.50: 'median', 0.75: 'u_quartile', 0.90: 'u_whisker'}

    def __get_quantiles(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Determines the daily quantiles of a series.

        :return:
        """

        calc: pd.DataFrame = frame.groupby(by=['datestr']).quantile(q=self.__q, numeric_only=True)

        # Set datestr as a normal field
        calc.reset_index(drop=False, inplace=True, col_level=1,
                         level=['datestr'], col_fill='indices')

        # The above addresses 1 of the three index fields created by group by.  Next
        # set the final index field, the field of quantile points, as a normal field.
        calc.reset_index(drop=False, inplace=True)

        # Pivot about the field of quantile points, named 'index'.
        matrix = calc.pivot(index=['datestr'], columns='index', values='measure')
        matrix.reset_index(drop=False, inplace=True)

        return matrix

    def exc(self, data: pd.DataFrame):
        """

        :param data: datestr | measure
        :return:
        """

        matrix = self.__get_quantiles(frame=data[['datestr', 'measure']])
        matrix.rename(columns=self.__q_points, inplace=True)

        return matrix
