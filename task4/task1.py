from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

from processors import BaseProcessor


class ErroneousRecordsProcessor(BaseProcessor):
    """
    Task 1: Erroneous records

        Nothing is perfect neither is the bidding system. From time to time something goes wrong
    in the bidding file generator in the partner's side, so it includes corrupted records in the stream.
        A record is corrupted if the value in the third column is not a number but a text
    with this format: ERROR_(.*)

        The task here is to filter out the corrupted records from the input and count how many occurred
    from the given type in the given hour.
        The output should be formatted as comma separated lines containing the date (with hour precision),
    the error message and the count of such messages in the given hour.

    Example: [05-21-07-2016,ERROR_NO_BIDS_FOR_HOTEL,1]
    """

    def __init__(self, spark_context, file_path):
        super().__init__(spark_context, file_path)
        self.header = [
            "MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA",
            "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE"
        ]
        self._process_dataset()

    def count_errors(self, n_rows_to_display: int = 10):
        errors_df = self._filter_errors()
        print(f'{errors_df.count()}/{self.df.count()} records corrupted')
        grouped_errors = self._group_errors_by_hour(errors_df)
        if n_rows_to_display is None or n_rows_to_display < 1 or not isinstance(n_rows_to_display, int):
            print(f'Displaying all {len(grouped_errors)} error rows')
            n_rows_to_display = len(grouped_errors)
        else:
            print(f'Displaying first {n_rows_to_display} error rows')
        print('\n'.join(grouped_errors[:n_rows_to_display]))

    def get_clean_records(self):
        """Returns dataframe without erroneous records"""
        return self.df.filter(~self._error_condition())

    def _filter_errors(self):
        error_df = self.df.filter(self._error_condition())
        return error_df

    def _error_condition(self):
        return self.df[self.df.columns[2]].rlike('ERROR.*')

    def _group_errors_by_hour(self, errors_df):
        group_by_cols = self.df.columns[1:3]
        records = errors_df.groupby(*group_by_cols).count().collect()
        output = []
        for row in records:
            output.append(f"{row[group_by_cols[0]]}, {row[group_by_cols[1]]}, {row['count']}")
        return output

    def _process_dataset(self):
        super()._process_dataset()
        timestamp_col_name = self.header[1]
        self.df = self.df.withColumn(
            timestamp_col_name,
            to_timestamp(self.df[timestamp_col_name], format='HH-dd-MM-yyyy')
        )


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()
    errors = ErroneousRecordsProcessor(spark, 'input/bids.txt')
    errors.count_errors()
    spark.stop()
