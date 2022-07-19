import json
import pandas as pd
from airflow.providers.http.operators.http import SimpleHttpOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def create_data_frame(spark: SparkSession, payload, schema):
    pandas_df = pd.DataFrame(pd.json_normalize(payload, max_level=0))
    return spark.createDataFrame(data=pandas_df, schema=schema)


def get_session() -> 'SparkSession':
    builder = SparkSession.builder.master('local[2]').appName('exemplo')
    return builder.getOrCreate()


class BrandExtraction(SimpleHttpOperator):

    def __init__(self, **kwargs):
        super(BrandExtraction, self).__init__(
            http_conn_id='example_conn',
            endpoint='posts',
            method='GET',
            **kwargs
        )

    def execute(self, context):
        response = super().execute(context)

        schema = StructType([
            StructField('userId', IntegerType(), False),
            StructField('id', IntegerType(), False),
            StructField('title', StringType(), False),
            StructField('body', StringType(), False),
        ])

        spark = get_session()
        # df = create_data_frame(spark, json.loads(response), schema)
        # df.write.format('json').json('posts.json')

        df = spark.read.json('posts.json', schema=schema)
        df.createTempView('vw_posts')
        spark.sql('select * from vw_posts').show()
