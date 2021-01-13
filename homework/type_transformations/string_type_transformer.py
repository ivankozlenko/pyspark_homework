from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        for col_name, expected_type in zip(dataframe.columns, expected_schema):
            dataframe = dataframe.withColumn(col_name, dataframe[col_name].cast(expected_type.dataType))
            dataframe.schema[col_name].nullable = expected_type.nullable  # TODO: doesn't work
        return dataframe
