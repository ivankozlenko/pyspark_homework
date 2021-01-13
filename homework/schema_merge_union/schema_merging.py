from pyspark.sql import DataFrame
from pyspark.sql import functions


class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        col_order = dataframe1.dtypes + [x for x in dataframe2.dtypes if x not in dataframe1.dtypes]
        col_mapping = self._get_col_mapping(dataframe1, dataframe2)

        # add missing columns for each dataframe
        dataframe1 = self._process_dataframe(dataframe1, 0, col_mapping, col_order)
        dataframe2 = self._process_dataframe(dataframe2, 1, col_mapping, col_order)

        return dataframe1.unionByName(dataframe2)

    @staticmethod
    def _has_same_name_with_different_type(col_name: str, col_mapping: dict) -> bool:
        '''If column with given has different data types in given dataframes'''
        return ((col_name in col_mapping[0]) and (col_name in col_mapping[1])) and \
               (col_mapping[0][col_name] != col_mapping[1][col_name])

    @staticmethod
    def _get_col_mapping(df1: DataFrame, df2: DataFrame) -> dict:
        col_mapping = {
            0: dict(df1.dtypes),
            1: dict(df2.dtypes)
        }
        return col_mapping

    @staticmethod
    def _rename_col_with_type(col_name, df_index, col_mapping):
        return f'{col_name}_{col_mapping[df_index][col_name]}'

    def _process_dataframe(self, df: DataFrame, index: int, col_mapping: dict, col_order: list):
        for col_name, col_type in col_order:
            if (col_name, col_type) in df.dtypes:
                if self._has_same_name_with_different_type(col_name, col_mapping):
                    df = df.withColumnRenamed(
                        col_name,
                        self._rename_col_with_type(col_name, index, col_mapping)
                    )
            else:
                if self._has_same_name_with_different_type(col_name, col_mapping):
                    # using XOR on index to get the other index
                    new_col_name = self._rename_col_with_type(col_name, index ^ 1, col_mapping)
                else:
                    new_col_name = col_name
                df = df.withColumn(
                    new_col_name,
                    functions.lit(None).cast(col_mapping[index ^ 1][col_name])
                )
        return df
