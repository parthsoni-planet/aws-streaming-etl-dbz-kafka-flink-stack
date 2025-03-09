import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings, ExplainDetail)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf


def sql_operations():
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

    t_env.sql_query("SELECT * FROM %s" % table) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   id |                           data |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |
    # | +I |                    2 | {"name": "hello", "tel": 13... |
    # | +I |                    3 | {"name": "world", "tel": 12... |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |
    # +----+----------------------+--------------------------------+

    # execute sql statement
    @udtf(result_types=[DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()])
    def parse_data(data: str):
        json_data = json.loads(data)
        yield json_data['name'], json_data['tel'], json_data['addr']['country']

    t_env.create_temporary_function('parse_data', parse_data)
    t_env.execute_sql(
        """
        SELECT *
        FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
        """ % table
    ).print()
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+
    # | op |                   id |                           data |                           name |         tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |                          Flink |         123 |                        Germany |
    # | +I |                    2 | {"name": "hello", "tel": 13... |                          hello |         135 |                          China |
    # | +I |                    3 | {"name": "world", "tel": 12... |                          world |         124 |                            USA |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |                        PyFlink |          32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+

    # explain sql plan
    print(t_env.explain_sql(
        """
        SELECT *
        FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
        """ % table
    ))
    # == Abstract Syntax Tree ==
    # LogicalProject(id=[$0], data=[$1], name=[$2], tel=[$3], country=[$4])
    # +- LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
    #    :- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]])
    #    +- LogicalTableFunctionScan(invocation=[parse_data($cor1.data)], rowType=[RecordType:peek_no_expand(VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)])
    #
    # == Optimized Physical Plan ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
    #
    # == Optimized Execution Plan ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

    # explain sql plan with advice
    print(t_env.explain_sql(
        """
        SELECT *
        FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
        """ % table, ExplainDetail.PLAN_ADVICE
    ))
    # == Abstract Syntax Tree ==
    # LogicalProject(id=[$0], data=[$1], name=[$2], tel=[$3], country=[$4])
    # +- LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
    #    :- LogicalTableScan(table=[[*anonymous_python-input-format$10*]])
    #    +- LogicalTableFunctionScan(invocation=[parse_data($cor2.data)], rowType=[RecordType:peek_no_expand(VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)])
    #
    # == Optimized Physical Plan With Advice ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- TableSourceScan(table=[[*anonymous_python-input-format$10*]], fields=[id, data])
    #
    # No available advice...
    #
    # == Optimized Execution Plan ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- TableSourceScan(table=[[*anonymous_python-input-format$10*]], fields=[id, data])



if __name__ == '__main__':
    sql_operations()