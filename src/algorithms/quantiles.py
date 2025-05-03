import logging

import cudf

class Quantiles:
    """
    Calculates daily quantiles per gauge
    """

    def __init__(self):
        """

        """

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

    def exc(self, data: cudf.DataFrame) -> cudf.DataFrame:
        """

        :param data: A data frame consisting of fields ['date', 'measure'] <b>only</b>.<br>
        :return:
        """

        sections = [self.__quantiles(data=data, quantile=quantile) for quantile, _ in self.__q_points.items()]
        instances = cudf.concat(sections, axis=1, ignore_index=False)

        logging.info(instances.head())

        return instances
