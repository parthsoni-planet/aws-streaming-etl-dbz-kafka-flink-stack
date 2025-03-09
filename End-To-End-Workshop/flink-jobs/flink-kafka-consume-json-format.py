import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json

def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    kafka_producer = FlinkKafkaProducer(
        topic='test_json_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'kafka1:29092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute("Test")


def read_from_kafka(env):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_json_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka1:29092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()

    env.add_source(kafka_consumer).print()
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:/opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar")
    env.add_jars("file:/opt/flink/lib/kafka-clients-3.2.3.jar")

    # env.add_jars("file:/home/pyflink/flink-connector-kafka_2.11-1.14.6.jar")
    # env.add_jars("file:/home/pyflink/kafka-clients-2.4.1.jar")

    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)