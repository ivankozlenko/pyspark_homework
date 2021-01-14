from typing import List

from pyspark.sql import DataFrame


class Unpivot:
    """
    Class provides unpivoting of some columns in dataset.
    For example for next dataset:
    +---+-----+-----+-----+-----+
    | id| name|10.02|20.02|28.02|
    +---+-----+-----+-----+-----+
    |  1| Ivan|  0.1|  0.1|  0.7|
    |  2|Maria|  0.2|  0.5|  0.9|
    +---+-----+-----+-----+-----+

    if we will consider `id` and `name` as constant columns, and columns 10.02, 20.02, 28.02 as dates,
    and other values as score it should provide next result:

    +---+-----+-----+-----+
    | id| name| date|score|
    +---+-----+-----+-----+
    |  1| Ivan|10.02|  0.1|
    |  1| Ivan|28.02|  0.7|
    |  1| Ivan|20.02|  0.1|
    |  2|Maria|10.02|  0.2|
    |  2|Maria|28.02|  0.9|
    |  2|Maria|20.02|  0.5|
    +---+-----+-----+-----+

    See spark sql function `stack`.
    """

    def __init__(self, constant_columns: List[str], key_col='', value_col=''):
        self.constant_columns = constant_columns
        self.key_col = key_col
        self.value_col = value_col

    def unpivot(self, dataframe: DataFrame) -> DataFrame:
        remaining_cols = [col for col in dataframe.columns if col not in self.constant_columns]
        stacked_cols = [f"'{remaining_col_name}', `{remaining_col_name}`"  # ` - quotes around value to return value!
                        for remaining_col_name in remaining_cols]
        stacked_cols_expr = ", ".join(stacked_cols)
        stack_sql_expr = f"stack({len(remaining_cols)}, {stacked_cols_expr}) " \
                         f"as ({self.key_col}, {self.value_col})"
        select = self.constant_columns
        if stacked_cols_expr:
            select.append(stack_sql_expr)
        df = dataframe.selectExpr(*select)
        return df
