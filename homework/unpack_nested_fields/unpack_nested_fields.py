from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    def unpack_nested(self, dataframe: DataFrame):
        for col_name, col_type in dataframe.dtypes:
            if col_type.startswith('array<'):
                dataframe = self._unpack_array(dataframe, col_name)
            elif col_type.startswith('struct<'):
                dataframe = self._unpack_struct(dataframe, col_name)
        return dataframe

    def _unpack_array(self, df: DataFrame, array_col_name: str):
        df = df.withColumn(array_col_name, explode(df[array_col_name]))
        return self.unpack_nested(df)

    def _unpack_struct(self, df: DataFrame, col_name):
        sub_df = df.select(col_name + '.*')
        for subcol_name in sub_df.columns:
            df = df.withColumn(f'{col_name}_{subcol_name}', df[col_name][subcol_name])
        df = df.drop(col_name)
        return self.unpack_nested(df)
