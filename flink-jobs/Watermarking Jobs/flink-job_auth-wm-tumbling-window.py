import sys

import argparse
from typing import Iterable

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    output_path = known_args.output

    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    json_collection = [
        { "acquirer_name": "PayPal",     "event_order": 1},
        { "acquirer_name": "MasterCard", "event_order": 2},  # Out-of-order
        { "acquirer_name": "PayPal",     "event_order": 3},  # Out-of-order
        { "acquirer_name": "MasterCard", "event_order": 4},  # Out-of-order
        { "acquirer_name": "PayPal",     "event_order": 5},  # In-order
        { "acquirer_name": "MasterCard", "event_order": 8},  # In-order
        { "acquirer_name": "MasterCard", "event_order": 9},  # In-order
        { "acquirer_name": "PayPal",     "event_order": 15}  # In-order
    ]


    def json_to_tuple(json_data):
        return (json_data["acquirer_name"], json_data["event_order"])

    tuple_collection = [json_to_tuple(json_obj) for json_obj in json_collection]

    # define the watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())
    
    ds = env.from_collection(tuple_collection, type_info=Types.TUPLE([
                            Types.STRING(),
                            Types.INT()
                        ]))

    ds = ds.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(TumblingEventTimeWindows.of(Time.milliseconds(5))) \
        .process(CountWindowProcessFunction(),
                 Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()