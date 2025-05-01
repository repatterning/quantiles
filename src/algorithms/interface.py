import logging

import pyspark.sql
import pyspark.sql.functions as pf
import pyspark.sql.types as pt
import pyspark.storagelevel

import src.elements.partitions as pr

class Interface:

    def __init__(self, spark: pyspark.sql.SparkSession, partitions: list[pr.Partitions] ):

        self.__spark = spark
        self.__partitions = partitions

    def __experiment(self, partition: pr.Partitions):

        frame = (self.__spark.read.format('csv')
         .option('header', 'true')
         .option('encoding', 'UTF-8').load(path=partition.uri))
        frame.persist(storageLevel=pyspark.StorageLevel.MEMORY_ONLY)

        logging.info(frame.rdd.getNumPartitions())

        frame.show()

        data = frame.withColumn(
            'date', pf.to_date(frame['timestamp'].cast(dataType=pt.TimestampType())))

        logging.info(data.rdd.getNumPartitions())

        data.show()


    def exc(self):

        map(self.__experiment, self.__partitions[:4])


