package iot.basics.operators;

import iot.basics.entities.*;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.Iterator;


public class MeanPerformanceTracking {

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

        DataStream<ValveState> valveState = env.addSource(new ValveStateSource(100000, 0.01))
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

        DataStream<MeanValveBehavior> meanPerformance = measurements.coGroup(valveState)
                .where(m ->m.getSensorId())
                .equalTo(v -> v.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.seconds(25)))
                .apply(new CoGroupFunction<SensorMeasurement, ValveState, MeanValveBehavior>() {
                    @Override
                    public void coGroup(Iterable<SensorMeasurement> measurements, Iterable<ValveState> states, Collector<MeanValveBehavior> collector) throws Exception {
                        int total = 0;
                        int totaltmp = 0;
                        double open = 0.0;
                        double closed = 0.0;
                        double obstructed = 0.0;
                        double temperature = 0.0;

                        Iterator<ValveState> it = states.iterator();
                        while (it.hasNext()){
                            total++;
                            switch (it.next().getValue()){
                                case OPEN:
                                    open++;
                                    break;
                                case CLOSE:
                                    closed++;
                                    break;
                                case OBSTRUCTED:
                                    obstructed++;
                                    break;
                            }
                        }

                        Iterator<SensorMeasurement> its = measurements.iterator();
                        while (its.hasNext()){
                            totaltmp++;
                            temperature += its.next().getValue();
                        }

                        if(total > 0 && totaltmp > 0)
                            collector.collect(new MeanValveBehavior(open/total, closed/total,
                                obstructed/total,temperature/totaltmp));

                    }
                });

        meanPerformance.print();

        env.execute();
    }
}