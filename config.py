"""
Module config
"""
import os


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.quantiles_ = os.path.join(self.warehouse, 'quantiles')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
