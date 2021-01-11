import re

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_replace
from typing import List


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError
        self.pattern = '|'.join(map(re.escape, chars))
        self.replacement = replacement

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        df = dataframe.withColumn(
            target_column,
            regexp_replace(dataframe[source_column], self.pattern, self.replacement),
        )
        df = df.drop(source_column)
        return df
