from pyspark.sql import SparkSession

from processors import BaseProcessor


MOTELS_FILE = 'input/motels.txt'


class MotelsProcessor(BaseProcessor):
    """
    Task 4: Load motels
    Load motels data and prepare it for joining with bids.
    Hint: we want to enrich the bids data with motel names, so you'll probably need the
    motel id and motel name as well.
    """

    def get_motels(self):
        df = self.df[[*self.df.columns[:3]]]
        header = ["MotelID", "MotelName", "Country"]
        for old_col, new_col in zip(df.columns, header):
            df = df.withColumnRenamed(old_col, new_col)
        return df


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    motels = MotelsProcessor(spark, MOTELS_FILE)
    motels.get_motels().show()
    spark.stop()
