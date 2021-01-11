from typing import Union, List

from pyspark.sql import DataFrame


class Deduplicator:
    """
    Current class provides possibilities to remove duplicated rows in data depends on provided primary key.
    If no primary keys were provided should be removed only identical rows.
    """

    def deduplicate(self, primary_keys: Union[str, List[str]], dataframe: DataFrame) -> DataFrame:
        if primary_keys:
            if isinstance(primary_keys, list):
                return dataframe.drop_duplicates(subset=primary_keys)
            else:
                return dataframe.drop_duplicates(subset=[primary_keys])
        else:
            return dataframe.drop_duplicates()