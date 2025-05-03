import logging

import cudf


class Extrema:
    """
    Per gauge, determines the daily minimum and maximum levels
    """

    def __init__(self):
        """

        """

        self.__metrics = ['minimum', 'maximum']

    def __extrema(self, data: cudf.DataFrame, calculate: str) -> cudf.DataFrame:
        """

        :param data:
        :param calculate:
        :return:
        """

        logging.info(calculate)

        match calculate:
            case 'maximum':
                metrics: cudf.DataFrame = data.groupby(by='date', as_index=True, axis=0).max()
            case 'minimum':
                metrics: cudf.DataFrame = data.groupby(by='date', as_index=True, axis=0).min()
            case _:
                raise ValueError(f'Unknown calculation request: {calculate}.  The valid terms are maximum and minimum.')

        return metrics.rename(columns={'measure': calculate})

    def exc(self, data: cudf.DataFrame) -> cudf.DataFrame:
        """

        :param data:
        :return:
        """

        sections = [self.__extrema(data=data, calculate=metric) for metric in self.__metrics]
        instances = cudf.concat(sections, axis=1, ignore_index=False)

        logging.info(instances.head())

        return instances
