package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FahrenheitNCelsius extends JobUtils {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        SensorMeasurement fahrenheit = new SensorMeasurement();
        DataStream<SensorMeasurement> farenheit = measurements.flatMap(new FlatMapFunction<SensorMeasurement, SensorMeasurement>() {
            @Override
            public void flatMap(SensorMeasurement m, Collector<SensorMeasurement> collector) throws Exception {
                fahrenheit.setSensorId(m.getSensorId());
                fahrenheit.setUnits("FAHRENHEIT");
                fahrenheit.setValue(m.getValue()*1.8 + 32);
                fahrenheit.setTimestamp(m.getTimestamp());
                collector.collect(m);
                collector.collect(fahrenheit);
            }
        });

        farenheit.print();

        env.execute();
    }
}
