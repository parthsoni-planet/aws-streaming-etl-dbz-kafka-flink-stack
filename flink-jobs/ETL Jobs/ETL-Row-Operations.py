import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings, ExplainDetail)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf

def row_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "China", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    # map operation
    @udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                    DataTypes.FIELD("country", DataTypes.STRING())]))
    def extract_country(input_row: Row):
        data = json.loads(input_row.data)
        return Row(input_row.id, data['addr']['country'])

    table.map(extract_country) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   id |                        country |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 |                        Germany |
    # | +I |                    2 |                          China |
    # | +I |                    3 |                          China |
    # | +I |                    4 |                          China |
    # +----+----------------------+--------------------------------+

    # flat_map operation
    @udtf(result_types=[DataTypes.BIGINT(), DataTypes.STRING()])
    def extract_city(input_row: Row):
        data = json.loads(input_row.data)
        yield input_row.id, data['addr']['city']

    table.flat_map(extract_city) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   f0 |                             f1 |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 |                         Berlin |
    # | +I |                    2 |                       Shanghai |
    # | +I |                    3 |                        NewYork |
    # | +I |                    4 |                       Hangzhou |
    # +----+----------------------+--------------------------------+

    # aggregate operation
    class CountAndSumAggregateFunction(AggregateFunction):

        def get_value(self, accumulator):
            return Row(accumulator[0], accumulator[1])

        def create_accumulator(self):
            return Row(0, 0)

        def accumulate(self, accumulator, input_row):
            accumulator[0] += 1
            accumulator[1] += int(input_row.tel)

        def retract(self, accumulator, input_row):
            accumulator[0] -= 1
            accumulator[1] -= int(input_row.tel)

        def merge(self, accumulator, accumulators):
            for other_acc in accumulators:
                accumulator[0] += other_acc[0]
                accumulator[1] += other_acc[1]

        def get_accumulator_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("cnt", DataTypes.BIGINT()),
                 DataTypes.FIELD("sum", DataTypes.BIGINT())])

        def get_result_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("cnt", DataTypes.BIGINT()),
                 DataTypes.FIELD("sum", DataTypes.BIGINT())])

    count_sum = udaf(CountAndSumAggregateFunction())
    table.add_columns(
            col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
            col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
            col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
         .group_by(col('country')) \
         .aggregate(count_sum.alias("cnt", "sum")) \
         .select(col('country'), col('cnt'), col('sum')) \
         .execute().print()
    # +----+--------------------------------+----------------------+----------------------+
    # | op |                        country |                  cnt |                  sum |
    # +----+--------------------------------+----------------------+----------------------+
    # | +I |                          China |                    3 |                  291 |
    # | +I |                        Germany |                    1 |                  123 |
    # +----+--------------------------------+----------------------+----------------------+

    # flat_aggregate operation
    class Top2(TableAggregateFunction):

        def emit_value(self, accumulator):
            for v in accumulator:
                if v:
                    yield Row(v)

        def create_accumulator(self):
            return [None, None]

        def accumulate(self, accumulator, input_row):
            tel = int(input_row.tel)
            if accumulator[0] is None or tel > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = tel
            elif accumulator[1] is None or tel > accumulator[1]:
                accumulator[1] = tel

        def get_accumulator_type(self):
            return DataTypes.ARRAY(DataTypes.BIGINT())

        def get_result_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("tel", DataTypes.BIGINT())])

    top2 = udtaf(Top2())
    table.add_columns(
            col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
            col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
            col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
        .group_by(col('country')) \
        .flat_aggregate(top2) \
        .select(col('country'), col('tel')) \
        .execute().print()
    # +----+--------------------------------+----------------------+
    # | op |                        country |                  tel |
    # +----+--------------------------------+----------------------+
    # | +I |                          China |                  135 |
    # | +I |                          China |                  124 |
    # | +I |                        Germany |                  123 |
    # +----+--------------------------------+----------------------+


if __name__ == '__main__':
    row_operations()