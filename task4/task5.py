from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from task1 import ErroneousRecordsProcessor, BIDS_FILE
from task2 import ExchangeRatesProcessor, EXCHANGE_RATES_FILE
from task3 import BidsProcessor
from task4 import MotelsProcessor, MOTELS_FILE


class DataProcessor:
    """
    Task 5: Finally enrich the data and find the maximum
        Motels.home wants to identify rich markets so it is interested where the advertisement is the most expensive
    so we are looking for maximum values.

        Join the bids with motel names.

        As a final output we want to find and only keep the records which have the maximum prices for a given
    MotelId/BidDate. When determining the maximum if the same price appears twice then keep all objects
    you found with the given price.

    Example:

    from records
    0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50
    0000001,Fantastic Hostel,2016-06-02 11:00,US,1.50
    0000001,Fantastic Hostel,2016-06-02 11:00,CA,1.15
    0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,MX,1.10
    0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,US,1.20
    0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30

    you will have to keep
    0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50
    0000001,Fantastic Hostel,2016-06-02 11:00,US,1.50
    0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30
    """

    def __init__(self, bids: BidsProcessor, motels: MotelsProcessor):
        self.bids = bids
        self.motels = motels

    def join_bids_with_motels(self):
        motels_df = self.motels.get_motels()
        bids_df = self.bids.get_bids()
        df = bids_df.join(motels_df, on='MotelID', how='left')

        # find max for given motel+date
        bids_by_date_df = df.groupby('MotelID', 'BidDate').agg(F.max('eur_bid').alias('max_bid_for_date'))

        # find max for given motel+date+loas
        bids_by_date_and_loas_df = df.groupby('MotelID', 'BidDate', 'loas') \
            .agg(F.max('eur_bid').alias('max_bid_for_date_and_loas'))

        # keep records where max bid for loas is also max for given date
        joined_df = bids_by_date_and_loas_df.join(bids_by_date_df, on=['MotelID', 'BidDate'], how='left')
        joined_df = joined_df.filter(joined_df.max_bid_for_date == joined_df.max_bid_for_date_and_loas).sort('BidDate')

        # process output
        joined_df = joined_df.withColumnRenamed('max_bid_for_date', 'max_bid')
        result_df = joined_df.join(df[['MotelID', 'MotelName']].drop_duplicates(), on='MotelID', how='left')
        result_df = result_df[['MotelID', 'MotelName', 'BidDate', 'loas', 'max_bid']].drop_duplicates() \
            .sort('MotelID', 'BidDate')

        # TODO: create two modes for date aggregation? e.g. timestamp-based (implemented) and date-based
        # TODO: (squash bids from the same day into single record and get max by day, not by timestamp)
        return result_df


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    errors = ErroneousRecordsProcessor(spark, BIDS_FILE)
    exchange_rates = ExchangeRatesProcessor(spark, EXCHANGE_RATES_FILE)
    bids = BidsProcessor(errors, exchange_rates)
    motels = MotelsProcessor(spark, MOTELS_FILE)
    data = DataProcessor(bids, motels)
    data.join_bids_with_motels().show()
    spark.stop()
