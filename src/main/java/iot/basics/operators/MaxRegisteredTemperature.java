package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MaxRegisteredTemperature {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<SensorMeasurement> maxRegistered = measurements
                .keyBy("sensorId")
                .reduce((a, b) -> a.getValue() > b.getValue() ? a : b)
                .filter(m -> m.getSensorId() == 11080);

        maxRegistered.print();

        env.execute();
    }
}
