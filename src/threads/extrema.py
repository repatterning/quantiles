
import pandas as pd

class Extrema:

    def __init__(self):

        self.__rename = {'min': 'minimum', 'max': 'maximum'}

    @staticmethod
    def __get_extrema(frame: pd.DataFrame) -> pd.DataFrame:
        """
        Determines each day's minimum & maximum measurements, per sequence.

        :return:
            A CUDF data frame of extrema
        """

        calc: pd.DataFrame = frame.groupby(
            by=['datestr']).agg(['min', 'max'])

        calc.reset_index(drop=False, inplace=True, col_level=1,
                         level=['datestr'], col_fill='indices')
        matrix = calc.set_axis(labels=calc.columns.get_level_values(level=1), axis=1)

        return matrix

    def exc(self, data: pd.DataFrame) -> pd.DataFrame:

        matrix = self.__get_extrema(frame=data[['datestr', 'measure']])
        matrix.rename(columns=self.__rename, inplace=True)

        return matrix
