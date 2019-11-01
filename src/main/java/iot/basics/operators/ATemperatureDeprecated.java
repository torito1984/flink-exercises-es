package iot.basics.operators;

import iot.basics.entities.MAValue;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ATemperatureDeprecated {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<Tuple3<Long, Long, Double>> maxRegistered = measurements
                .keyBy("sensorId")
                .fold(new MAValue(), new FoldFunction<SensorMeasurement, MAValue>() {
                    @Override
                    public MAValue fold(MAValue previous, SensorMeasurement o) throws Exception {
                        previous.setSensorId(o.getSensorId());
                        previous.setTimestamp(o.getTimestamp());
                        previous.setCount(previous.getCount()+1);
                        previous.setCum(previous.getCum() + o.getValue());
                        return previous;
                    }
                })
                .map(new MapFunction<MAValue, Tuple3<Long, Long, Double>>() {
                     @Override
                     public Tuple3<Long, Long, Double> map(MAValue ma) throws Exception {
                         return new Tuple3<>(ma.getSensorId(), ma.getTimestamp(), ma.getCum() / ma.getCount());
                     }
                })
                .filter(m -> m.f0 == 11080);

        maxRegistered.print();

        env.execute();
    }
}
