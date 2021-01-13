from pyspark import Row
from pyspark.sql import SparkSession

from homework.schema_merge_union.schema_merging import SchemaMerging


def test_merge_schemas_simple(spark_session: SparkSession):
    df1 = spark_session.createDataFrame(
        [
            (0, "potato", "0.5", "100"),
            (1, "onion", "0.5", "150"),
        ],
        ["id", "product", "weight", "price"]
    )
    df2 = spark_session.createDataFrame(
        [
            (2, "CyberPunk2077", 1, "3000"),
            (3, "TENET", 1, "2000"),
        ],
        ["id", "product", "amount", "price"]
    )
    merger = SchemaMerging()
    result = merger.union(df1, df2)
    assert result.collect() == [Row(id=0, product='potato', weight='0.5', price='100', amount=None),
                                Row(id=1, product='onion', weight='0.5', price='150', amount=None),
                                Row(id=2, product='CyberPunk2077', weight=None, price='3000', amount=1),
                                Row(id=3, product='TENET', weight=None, price='2000', amount=1)]


def test_merge_schemas_no_difference(spark_session: SparkSession):
    df1 = spark_session.createDataFrame(
        [
            (0, "potato", "0.5", "100"),
            (1, "onion", "0.5", "150"),
        ],
        ["id", "product", "weight", "price"]
    )
    df2 = spark_session.createDataFrame(
        [
            (2, "apple", "1", "300"),
            (3, "pineapple", "1", "200"),
        ],
        ["id", "product", "weight", "price"]
    )
    merger = SchemaMerging()
    result = merger.union(df1, df2)
    assert result.collect() == [Row(id=0, product='potato', weight='0.5', price='100'),
                                Row(id=1, product='onion', weight='0.5', price='150'),
                                Row(id=2, product='apple', weight='1', price='300'),
                                Row(id=3, product='pineapple', weight='1', price='200')]


def test_merge_schemas_diff_types(spark_session: SparkSession):
    df1 = spark_session.createDataFrame(
        [
            (0, "potato", "0.5", 100),
            (1, "onion", "0.5", 150),
        ],
        ["id", "product", "weight", "price"]
    )
    df2 = spark_session.createDataFrame(
        [
            (2, "apple", "1", "300"),
            (3, "pineapple", "1", "200"),
        ],
        ["id", "product", "weight", "price"]
    )
    merger = SchemaMerging()
    result = merger.union(df1, df2)
    assert result.collect() == [Row(id=0, product='potato', weight='0.5', price_bigint=100, price_string=None),
                                Row(id=1, product='onion', weight='0.5', price_bigint=150, price_string=None),
                                Row(id=2, product='apple', weight='1', price_bigint=None, price_string='300'),
                                Row(id=3, product='pineapple', weight='1', price_bigint=None, price_string='200')]


def test_merge_schemas_no_common(spark_session: SparkSession):
    df1 = spark_session.createDataFrame(
        [
            ('uuid1', "honda", "50000"),
            ('uuid2', "toyota", "60000"),
        ],
        ["uuid", "car", "mileage"]
    )
    df2 = spark_session.createDataFrame(
        [
            (2, "apple", "1", "300"),
            (3, "pineapple", "1", "200"),
        ],
        ["id", "product", "weight", "price"]
    )
    merger = SchemaMerging()
    result = merger.union(df1, df2)
    assert result.collect() == [Row(uuid='uuid1', car='honda', mileage='50000', id=None, product=None, weight=None, price=None),
                                Row(uuid='uuid2', car='toyota', mileage='60000', id=None, product=None, weight=None, price=None),
                                Row(uuid=None, car=None, mileage=None, id=2, product='apple', weight='1', price='300'),
                                Row(uuid=None, car=None, mileage=None, id=3, product='pineapple', weight='1', price='200')]
