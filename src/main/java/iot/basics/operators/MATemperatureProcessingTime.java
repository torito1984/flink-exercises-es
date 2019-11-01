package iot.basics.operators;

import iot.basics.entities.MAValue;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MATemperatureProcessingTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple3<Long, Long, Double>> meanRegistered = measurements
                .keyBy("sensorId")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<SensorMeasurement, MAValue, MAValue>() {
                    public MAValue createAccumulator() {
                        return new MAValue();
                    }

                    public MAValue add(SensorMeasurement o, MAValue previous) {
                        previous.setSensorId(o.getSensorId());
                        previous.setTimestamp((o.getTimestamp() > previous.getTimestamp()) ? o.getTimestamp() : previous.getTimestamp());
                        previous.setCount(previous.getCount() + 1);
                        previous.setCum(previous.getCum() + o.getValue());
                        return previous;
                    }

                    public MAValue getResult(MAValue emaValue) {
                        return emaValue;
                    }

                    public MAValue merge(MAValue a, MAValue b) {
                        MAValue out = new MAValue();
                        out.setSensorId(a.getSensorId());
                        out.setTimestamp((a.getTimestamp() < b.getTimestamp()) ? b.getTimestamp() : a.getTimestamp());
                        out.setCount(a.getCount() + b.getCount());
                        out.setCum(a.getCum() + b.getCum());
                        return out;
                    }
                }, new ProcessWindowFunction<MAValue, Tuple3<Long, Long, Double>, Tuple, TimeWindow>() {
                    public void process(Tuple tuple, Context context, Iterable<MAValue> iterable, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
                        MAValue value = iterable.iterator().next();
                        collector.collect(new Tuple3<>(value.getSensorId(), value.getTimestamp(), value.getCum()/value.getCount()));
                    }
                })
                .filter(m -> m.f0 == 11080);

        meanRegistered.print();

        env.execute();
    }
}
