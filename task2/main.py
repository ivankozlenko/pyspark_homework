import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat


def create_path(*args):
    return '/'.join(map(str, args))


DATASET_NAME = 'measurements_dataset.csv'
path = os.path.realpath(__file__).split('/')
dir_path = path[:-1]
data_path = create_path(*dir_path, 'data', DATASET_NAME)

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.csv(data_path)

# row count
print(f'Dataset has {df.count()} rows')

## write to various file formats

# orc
df.write.orc(create_path(*dir_path, 'outputs', 'dataset_as_orc'))

# parquet - TODO: lzo and lz4 do not work locally
for compression in [None, 'snappy', 'gzip', 'lzo', 'lz4']:
    df.write.parquet(
        create_path(*dir_path, 'outputs', f'dataset_as_parquet_{compression}'),
        compression=compression
    )

# sequence file
first_col, rest_cols = df.columns[0], df.columns[1:]
df.select(first_col, concat(*rest_cols))\
    .rdd.map(lambda x: (str(x[0]), str(x[1])))\
    .saveAsSequenceFile(create_path(*dir_path, 'outputs', 'dataset_as_sequencefile'))

# avro - TODO: doesn't work
df.write.format('avro').save(create_path(*dir_path, 'outputs', 'dataset_as_avro'))
