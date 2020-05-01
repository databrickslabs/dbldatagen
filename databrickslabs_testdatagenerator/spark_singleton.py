from pyspark.sql import SparkSession
import os

class SparkSingleton:
    """A singleton class on Datalib which returns one Spark instance"""

    @classmethod
    def get_instance(cls):
        """Create a Spark instance for Datalib.
        :return: A Spark instance
        """

        return SparkSession.builder.getOrCreate()
