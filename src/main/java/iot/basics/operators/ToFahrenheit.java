package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ToFahrenheit extends JobUtils {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<SensorMeasurement> farenheit = measurements.map( m -> {
            m.setValue(m.getValue()*1.8 + 32);
            m.setUnits("FAHRENHEIT");
            return m;
        });

        farenheit.print();

        env.execute();
    }
}
