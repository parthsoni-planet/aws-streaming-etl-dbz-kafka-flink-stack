import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings, ExplainDetail)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf

def column_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    # add columns
    table = table.add_columns(
        col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
        col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
        col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country'))

    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           data |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |                          Flink |                            123 |                        Germany |
    # | +I |                    2 | {"name": "hello", "tel": 13... |                          hello |                            135 |                          China |
    # | +I |                    3 | {"name": "world", "tel": 12... |                          world |                            124 |                            USA |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+

    # drop columns
    table = table.drop_columns(col('data'))
    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # rename columns
    table = table.rename_columns(col('tel').alias('telephone'))
    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                      telephone |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # replace columns
    table = table.add_or_replace_columns(
        concat(col('id').cast(DataTypes.STRING()), '_', col('name')).alias('id'))
    table.execute().print()
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                             id |                           name |                      telephone |                        country |
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                        1_Flink |                          Flink |                            123 |                        Germany |
    # | +I |                        2_hello |                          hello |                            135 |                          China |
    # | +I |                        3_world |                          world |                            124 |                            USA |
    # | +I |                      4_PyFlink |                        PyFlink |                             32 |                          China |
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+

if __name__ == '__main__':
    column_operations()