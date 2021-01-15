from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, udf, to_timestamp, round

from task1 import ErroneousRecordsProcessor, BIDS_FILE
from task2 import ExchangeRatesProcessor, EXCHANGE_RATES_FILE


class BidsProcessor:
    """
    Dealing with bids
        Now we can focus on the most important parts, the bids. In Task 1 you have read the original
    bids data. The first part is to get rid of the erroneous records and keep only the conforming
    ones which are not prefixed with ERROR_ strings.
        In this campaign Motel.home is focusing only on three countries: US,CA,MX so you'll have to
    only work with those three and also the records have to be transposed so one record will only
    contain price for one Losa.

    Example:

    original record:
    ["MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE"]
    [0000002,11-05-08-2016,0.92,1.68,0.81,0.68,1.59,,1.63,1.77,2.06,0.66,1.53,,0.32,0.88,0.83,1.01]

    keep only the three important ones
    0000002,11-05-08-2016,1.59,,1.77

    transpose the record and include the related Losa in a separate column
    0000002,11-05-08-2016,US,1.59
    0000002,11-05-08-2016,MX,
    0000002,11-05-08-2016,CA,1.77

    (This is closely related to SQL explode functionality)

    Somewhere in this task you have to do the following conversions/filters:
     - Convert USD to EUR. The result should be rounded to 3 decimal precision.
     - Convert dates to proper format - "yyyy-MM-dd HH:mm" instead of original "HH-dd-MM-yyyy"
     - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
    """

    def __init__(self, errors: ErroneousRecordsProcessor, exchange_rates: ExchangeRatesProcessor):
        self.errors = errors
        self.exchange_rates = exchange_rates
        self.bid_cols_subset = ["MotelID", "BidDate", "US", "MX", "CA"]

    def get_bids(self):
        df = self._filter_data()
        df = self._transpose_data(df)
        df = self._filter_missing_bids(df)
        df = self._exchange_date(df)
        df = self._calculate_bid(df)
        return df

    def _filter_data(self):
        df = self.errors.get_clean_records()
        df = df[[*self.bid_cols_subset]]
        return df

    def _transpose_data(self, df):
        # does unpivot function from homework suit better than explode() or not?
        main_cols = self.bid_cols_subset[:2]
        t_cols = self.bid_cols_subset[2:]
        stack_expr = ', '.join([f"'{key}', `{key}`" for key in t_cols])
        stack_sql_expr = f"stack({len(t_cols)}, {stack_expr}) as (loas, bid)"
        df = df.selectExpr(*main_cols, stack_sql_expr)
        return df

    def _filter_missing_bids(self, df):
        return df.filter(df['bid'].isNotNull())

    def _exchange_date(self, df):
        df = df.withColumn('date', to_date(df['BidDate']))
        mapping = self.exchange_rates.get_mapping()

        @udf
        def mapper(col):
            return mapping.get(col)

        df = df.withColumn('rate', mapper('date'))
        return df

    def _calculate_bid(self, df):
        df = df.withColumn('bid', df['bid'].cast('decimal(6,3)'))
        df = df.withColumn('eur_bid', round(df['bid'] * df['rate'], 3))
        df = df.withColumn('bid', df['eur_bid'].cast('decimal(6,3)'))
        df = df.withColumn('BidDate', to_timestamp(df['BidDate'], format='yyyy-MM-dd HH:mm'))
        df = df[['MotelID', 'BidDate', 'loas', 'eur_bid']]
        return df


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    errors = ErroneousRecordsProcessor(spark, BIDS_FILE)
    exchange_rates = ExchangeRatesProcessor(spark, EXCHANGE_RATES_FILE)
    bids = BidsProcessor(errors, exchange_rates)
    bids.get_bids().show()
    spark.stop()
