from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

from processors import BaseProcessor


class ExchangeRatesProcessor(BaseProcessor):
    """
    Task 2: Exchange rates
        As Motels.home is Europe based it is convenient to convert all types of currencies to EUR
    for the business analysts. In our example we have only USD so we will do only USD to EUR conversion.
        Here you have to read the currencies into a map where the dates are the keys
    and the related conversion rates are the values. Use this data as mapping source to be able
    to exchange the provided USD value to EUR on any given date/time.
    """

    def __init__(self, spark_context, file_path):
        super().__init__(spark_context, file_path)
        self.header = ["ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate"]
        self._process_dataset()

    def _process_dataset(self):
        super()._process_dataset()
        # saving date as separate column just in case
        self.df = self.df.withColumn(
            'ValidFrom_as_date',
            to_timestamp('ValidFrom', format='HH-dd-MM-yyyy')
        )
        self.df.withColumn(
            'ExchangeRate',
            self.df['ExchangeRate'].cast('decimal(4,3)')
        )

    def get_mapping(self):
        data = self.df[['ValidFrom', 'ExchangeRate']].distinct().collect()
        mapping = {row['ValidFrom']: row['ExchangeRate'] for row in data}
        return mapping


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    exchange_rates = ExchangeRatesProcessor(spark, 'input/exchange_rate.txt')
    exchange_rates.show_df()
    print(exchange_rates.get_mapping())
    spark.stop()
