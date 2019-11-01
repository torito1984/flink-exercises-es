package iot.basics.operators;

import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.entities.ValveState;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class StateTemperatureTracking {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();

        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorMeasurement>() {
                    private final long maxOutOfOrderness = 3500; // 3.5 seconds
                    private long currentMaxTimestamp;

                    public long extractTimestamp(SensorMeasurement sensorMeasurement, long previousElementTimestamp) {
                        long timestamp = sensorMeasurement.getTimestamp();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        DataStream<ValveState> valveState = env.addSource(new ValveStateSource(10000, 0.01))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ValveState>() {
                    private final long maxOutOfOrderness = 3500; // 3.5 seconds
                    private long currentMaxTimestamp;

                    public long extractTimestamp(ValveState sensorMeasurement, long previousElementTimestamp) {
                        long timestamp = sensorMeasurement.getTimestamp();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        // Join the two streams

        DataStream<Tuple2<SensorMeasurement, ValveState>> fullState = measurements.join(valveState)
                .where(m ->m.getSensorId())
                .equalTo(v -> v.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                .apply(new FlatJoinFunction<SensorMeasurement, ValveState, Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public void join(SensorMeasurement sensorMeasurement, ValveState valveState, Collector<Tuple2<SensorMeasurement, ValveState>> collector) throws Exception {
                        if((sensorMeasurement.getTimestamp() > valveState.getTimestamp()) && (sensorMeasurement.getTimestamp() - valveState.getTimestamp()) < 200){
                            collector.collect(new Tuple2<>(sensorMeasurement, valveState));
                        }
                    }
                });

        final OutputTag<Tuple2<SensorMeasurement, ValveState>> outputOpenClosed =
                new OutputTag<Tuple2<SensorMeasurement, ValveState>>("open-closed"){};
        final OutputTag<Tuple2<SensorMeasurement, ValveState>> outputObstructed =
                new OutputTag<Tuple2<SensorMeasurement, ValveState>>("obstructed"){};

        SingleOutputStreamOperator<Tuple2<SensorMeasurement, ValveState>> alertsStream = fullState
            .filter(m -> (m.f0.getValue() > 500 && m.f1.getValue() == SensorValue.State.OPEN) ||
                         (m.f0.getValue() > 550 && m.f1.getValue() == SensorValue.State.CLOSE) ||
                         (m.f1.getValue() == SensorValue.State.OBSTRUCTED))
            .process(new ProcessFunction<Tuple2<SensorMeasurement, ValveState>, Tuple2<SensorMeasurement, ValveState>>() {
                @Override
                public void processElement(Tuple2<SensorMeasurement, ValveState> m, Context context,
                                           Collector<Tuple2<SensorMeasurement, ValveState>> collector) throws Exception {
                    switch(m.f1.getValue()){
                        case OPEN:
                        case CLOSE:
                            context.output(outputOpenClosed, m);
                            break;
                        case OBSTRUCTED:
                            context.output(outputObstructed, m);
                    }
                }
            });

        // Alert different types of alerts in different ways
        alertsStream.getSideOutput(outputOpenClosed).map( m -> {
            SensorAlert alert = new SensorAlert();
            alert.setSensorId(m.f0.getSensorId());
            alert.setUnits("MALFUNCTIONING TEMPERATURE");
            alert.setValue(m.f0.getValue());
            alert.setTimestamp(m.f0.getTimestamp());
            alert.setLevel(2);
            return alert;
        }).print();

        alertsStream.getSideOutput(outputOpenClosed).map( m -> {
            SensorAlert alert = new SensorAlert();
            alert.setSensorId(m.f0.getSensorId());
            alert.setUnits("MALFUNCTIONING OBSTRUCTED");
            alert.setValue(m.f0.getValue());
            alert.setTimestamp(m.f0.getTimestamp());
            alert.setLevel(3);
            return alert;
        }).print();


        env.execute();
    }
}