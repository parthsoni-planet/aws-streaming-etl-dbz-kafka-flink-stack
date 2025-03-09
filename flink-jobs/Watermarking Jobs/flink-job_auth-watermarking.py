from pyflink.common import Time, WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from datetime import datetime

class Sum(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())
        state_ttl_config = StateTtlConfig \
            .new_builder(Time.seconds(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background() \
            .build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = 0

        # update the state's count
        current += value[2]
        self.state.update(current)

        # register an event time timer 2 seconds later
        ctx.timer_service().register_event_time_timer(ctx.timestamp() + 2000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        yield ctx.get_current_key(), self.state.value()


class MyTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        timestamp_str = value[0]
        timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        timestamp_int = int(timestamp_obj.timestamp())
        return timestamp_int


def event_timer_timer_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    json_collection = [
        {"transaction_creation_date": "2024-02-19T08:00:00", "acquirer_name": "PayPal",     "value": 110.1},
        {"transaction_creation_date": "2024-02-19T08:00:02", "acquirer_name": "MasterCard", "value": 30.2},  # Out-of-order
        {"transaction_creation_date": "2024-02-19T08:00:03", "acquirer_name": "PayPal",     "value": 20.0},  # Out-of-order
        {"transaction_creation_date": "2024-02-19T08:00:01", "acquirer_name": "MasterCard", "value": 53.1},  # Out-of-order
        {"transaction_creation_date": "2024-02-19T08:00:04", "acquirer_name": "PayPal",     "value": 13.1},  # In-order
        {"transaction_creation_date": "2024-02-19T08:00:03", "acquirer_name": "MasterCard", "value": 3.1},  # In-order
        {"transaction_creation_date": "2024-02-19T08:00:07", "acquirer_name": "MasterCard", "value": 16.1},  # In-order
        {"transaction_creation_date": "2024-02-19T08:00:05", "acquirer_name": "PayPal",     "value": 20.1}  # In-order
    ]


    def json_to_tuple(json_data):
        return (json_data["transaction_creation_date"], json_data["acquirer_name"], json_data["value"])

    tuple_collection = [json_to_tuple(json_obj) for json_obj in json_collection]

    ds = env.from_collection(tuple_collection, type_info=Types.TUPLE([
                            Types.STRING(),
                            Types.STRING(),
                            Types.FLOAT()
                        ]))

    ds = ds.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
                         .with_timestamp_assigner(MyTimestampAssigner()))

    # apply the process function onto a keyed stream
    ds.key_by(lambda value: value[1]) \
      .process(Sum()) \
      .print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    event_timer_timer_demo()
