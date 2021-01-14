from pyspark.sql import DataFrame
from pyspark.sql.functions import when, column, expr


class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """

    def __init__(self, primary_keys=None):
        self.primary_keys = primary_keys

    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        if self.primary_keys:
            data_keys = [k for k in old_dataframe.columns if k not in self.primary_keys]
        else:
            data_keys = old_dataframe.columns

        dfs = {}
        join_keys = self.primary_keys or []
        for df, df_alias in zip([old_dataframe, new_dataframe], ['old', 'new']):
            aliased_keys = [column(x).alias(f'{x}_{df_alias}') for x in join_keys + data_keys]
            select_keys = aliased_keys
            dfs[df_alias] = df.select(*select_keys)

        null_safe_keys = join_keys or data_keys
        null_safe_join_expr = [dfs['old'][f'{k}_old'].eqNullSafe(dfs['new'][f'{k}_new'])
                               for k in null_safe_keys]
        joined_df = dfs['old'].join(dfs['new'], on=null_safe_join_expr, how='outer')

        # equal if all data keys are equal
        sql_equal_expr = ' AND '.join([f'{k}_old = {k}_new' for k in data_keys])

        # inserted if all old fields are null
        sql_inserted_expr = ' AND '.join([f'{k}_old IS NULL' for k in data_keys])

        # deleted if all new fields are null
        sql_deleted_expr = ' AND '.join([f'{k}_new IS NULL' for k in data_keys])

        joined_df = joined_df.withColumn(
            'meta',
            when(expr(sql_equal_expr), 'not_changed') \
                .when(expr(sql_inserted_expr), 'inserted') \
                .when(expr(sql_deleted_expr), 'deleted') \
                .otherwise('changed')
        )

        for name in join_keys + data_keys:
            # move data from old columns to new columns, in order to save it when we drop old columns
            joined_df = joined_df.withColumn(
                f'{name}_new',
                when((joined_df['meta'] == 'deleted'), joined_df[f'{name}_old'])
                    .otherwise(joined_df[f'{name}_new'])
            )

        # drop old columns, remove 'new' suffix from remaining columns
        joined_df = joined_df.drop(*[f'{name}_old' for name in join_keys + data_keys])
        for name in data_keys:
            joined_df = joined_df.withColumnRenamed(f'{name}_new', name)

        return joined_df
