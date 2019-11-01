package iot.basics.operators;

import iot.basics.entities.SecondarySensorMeasurement;
import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.source.SecondarySensorMeasurementSource;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class SecondarySensor {

    public static void main(String[] args) throws Exception {

        final int MAX_TOLERANCE = 20;

        StreamExecutionEnvironment env = JobUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));
        DataStream<SecondarySensorMeasurement> secondaryMeasurements = env.addSource(new SecondarySensorMeasurementSource(100_000));

        // Map to a common API
        DataStream<SensorValue> primaryValues = measurements.map(m -> SensorValue.builder().sensorId(m.getSensorId())
                .timestamp(m.getTimestamp())
                .value(m.getValue())
                .units(m.getUnits())
                .type("PRIMARY")
                .build());

        DataStream<SensorValue> secondaryValues = secondaryMeasurements.map(m -> SensorValue.builder().sensorId(m.getSensorId())
                .timestamp(m.getTimestamp())
                .value(m.getValue())
                .units(m.getUnits())
                .type("SECONDARY")
                .build());

        // Union the streams and assign watermarks
        DataStream<SensorValue> allValues = primaryValues.union(secondaryValues)
          .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorValue>() {
            private final long maxOutOfOrderness = 3500; // 3.5 seconds
            private long currentMaxTimestamp;

            public long extractTimestamp(SensorValue sensorMeasurement, long previousElementTimestamp) {
                long timestamp = sensorMeasurement.getTimestamp();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }

            public Watermark getCurrentWatermark() {
                // return the watermark as current highest timestamp minus the out-of-orderness bound
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
          });


        DataStream<SensorAlert> sensorAlerts = allValues
          .keyBy("sensorId")
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .process(new ProcessWindowFunction<SensorValue, SensorAlert, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<SensorValue> iterable, Collector<SensorAlert> collector) throws Exception {
                double countPrimary = 0;
                double sumPrimary = 0;
                double countSecondary = 0;
                double sumSecondary = 0;

                long sensorId = 0;

                Iterator<SensorValue> it = iterable.iterator();
                while(it.hasNext()){
                    SensorValue i = it.next();
                    sensorId = i.getSensorId();
                    if(i.getType().equals("PRIMARY")){
                        countPrimary++;
                        sumPrimary += i.getValue();
                    }else{
                        countSecondary++;
                        sumSecondary += i.getValue();
                    }
                }

                double sensorDiff = Math.abs(sumPrimary/countPrimary - sumSecondary/countSecondary);
                if(sensorDiff > MAX_TOLERANCE){
                    SensorAlert alert = new SensorAlert();
                    alert.setTimestamp(context.window().getEnd());
                    alert.setSensorId(sensorId);
                    alert.setUnits("SENSOR DIFF CELSIUS");
                    alert.setValue(sensorDiff);
                    alert.setLevel(2);
                    collector.collect(alert);
                }
            }
        });

        //.filter(m -> m.getSensorId() == 11080)
        sensorAlerts.print();
        env.execute();

        /*DataStream<SensorMeasurement> maxRegistered = measurements
                .keyBy("sensorId")
                .maxBy("value")
                .filter(m -> m.getSensorId() == 11080);
        */
    }
}
