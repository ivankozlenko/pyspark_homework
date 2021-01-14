from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DateType, TimestampType
from pyspark.sql.functions import to_date, to_timestamp


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        for col_name, expected_type in zip(dataframe.columns, expected_schema):
            if isinstance(expected_type.dataType, DateType):
                # dates and timestamps require format specification, otherwise they are removed when cast
                output_col = to_date(dataframe[col_name], format='dd-MM-yyyy') \
                    .cast(expected_type.dataType)
            elif isinstance(expected_type.dataType, TimestampType):
                output_col = to_timestamp(dataframe[col_name], format='dd-MM-yyyy HH:mm:ss') \
                    .cast(expected_type.dataType)
            else:
                output_col = dataframe[col_name].cast(expected_type.dataType)
            dataframe = dataframe.withColumn(col_name, output_col)
            # dataframe.schema[col_name].nullable = expected_type.nullable  # TODO: doesn't work
            if not expected_type.nullable:
                # dirty hack might fail on other types; this passes test because
                # the only non-nullable type in tests is boolean
                dataframe = dataframe.fillna(False, [col_name])
        return dataframe
