from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time, WatermarkStrategy
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext, KeyedProcessFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import StateTtlConfig, Time, ValueStateDescriptor, functions

impression_type_info = Types.ROW_NAMED(
    ['impression_id','user_id','campaign_id','ad_id','device_type',
     'browser','event_timestamp','cost'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.LONG(), Types.DOUBLE()])

click_type_info = Types.ROW_NAMED(
    ['click_id','impression_id','user_id','event_timestamp'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()])

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("C:Program Files/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

impression_consumer = FlinkKafkaConsumer(
    topics='ad-impressions',
    deserialization_schema=JsonRowDeserializationSchema.builder()
    .type_info(type_info=impression_type_info).build(),
    properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-group'})

click_consumer = FlinkKafkaConsumer(
    topics='ad-clicks',
    deserialization_schema=JsonRowDeserializationSchema.builder()
    .type_info(type_info=click_type_info).build(),
    properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-group'})

class ImpressionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[6]  # event_timestamp

class ClickTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[3]  # event_timestamp

impressions = env.add_source(impression_consumer)\
    .assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(ImpressionTimestampAssigner()))

clicks = env.add_source(click_consumer)\
    .assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(ClickTimestampAssigner()))

class CTRCalculator(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        impressions = len(elements)
        clicks = sum(1 for element in elements) # Every element represents one impression.
        ctr = (clicks / impressions) * 100 if impressions > 0 else 0
        window_end = context.window().end
        out.collect((key, ctr, window_end))

ctr_stream = impressions.key_by(lambda x: x[2]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .process(CTRCalculator()) \
    .name("CTR Calculator")

class CTRAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.state_ttl_config = StateTtlConfig.Builder(Time.minutes(60)).update_never().build() # 1 hour TTL
        self.ctr_history_state: functions.ValueState = None

    def open(self, configuration: functions.Configuration):
        state_descriptor = ValueStateDescriptor("ctr_history", Types.LIST(Types.FLOAT()))
        state_descriptor.enable_time_to_live(self.state_ttl_config)
        self.ctr_history_state = self.runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx, out):
        campaign_id, current_ctr, window_end = value
        history = self.ctr_history_state.value()
        if history is None:
            history = []

        history.append(current_ctr)
        if len(history) > 10: # Keep last 10 values
            history = history[-10:]
        self.ctr_history_state.update(history)

        if len(history) >= 5:
            avg = sum(history) / len(history)
            std_dev = (sum([(x - avg) ** 2 for x in history]) / len(history)) ** 0.5
            if std_dev > 0 and abs(current_ctr - avg) > 2 * std_dev: #Adjust threshold as needed
                out.collect(f"ANOMALY: Campaign {campaign_id}, CTR {current_ctr:.2f}%, Avg {avg:.2f}, StdDev {std_dev:.2f}")

anomaly_stream = ctr_stream.key_by(lambda x: x[0]) \
    .process(CTRAnomalyDetector()) \
    .name("Anomaly Detector")

class DeviceEngagementCalculator(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        unique_users = set()
        for element in elements:
            unique_users.add(element[2]) #User ID
        num_clicks = len(elements)
        out.collect((key, len(unique_users), num_clicks, context.window().end))

device_stream = clicks.key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .process(DeviceEngagementCalculator()) \
    .name("Device Engagement Calculator")

ctr_stream.map(lambda x: (x[0], float(x[1]), int(x[2]))).add_sink(FlinkKafkaProducer(
    topic='ctr-metrics',
    serialization_schema=JsonRowSerializationSchema.builder()
    .with_type_info(Types.ROW_NAMED(['campaign_id', 'ctr', 'window_end'],[Types.STRING(), Types.FLOAT(), Types.LONG()])).build(),
    producer_config={'bootstrap.servers': 'kafka:9092'}))

anomaly_stream.add_sink(FlinkKafkaProducer(
    topic='anomaly-alerts',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': 'kafka:9092'}))

device_stream.map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]))).add_sink(FlinkKafkaProducer(
    topic='device-engagement',
    serialization_schema=JsonRowSerializationSchema.builder()
    .with_type_info(Types.ROW_NAMED(['device_type', 'unique_users', 'num_clicks', 'window_end'],[Types.STRING(), Types.INT(), Types.INT(), Types.LONG()])).build(),
    producer_config={'bootstrap.servers': 'kafka:9092'}))

env.execute("Ad Click Analytics")
