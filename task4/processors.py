class BaseProcessor:

    def __init__(self, spark_context, file_path, header: list = None):
        self.sc = spark_context
        self.df = self.sc.read.csv(file_path, header=None)
        self.header = header

    def show_df(self):
        self.df.show()

    def _process_dataset(self):
        for old_col, new_col in zip(self.df.columns, self.header):
            self.df = self.df.withColumnRenamed(old_col, new_col)

