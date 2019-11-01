package iot.basics.operators;

import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HeatAlert {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<SensorAlert> criticalAlerts = measurements
                .filter(m -> m.getValue() > 550)
                .map( m -> {
                    SensorAlert alert = new SensorAlert();
                    alert.setSensorId(m.getSensorId());
                    alert.setUnits(m.getUnits());
                    alert.setValue(m.getValue());
                    alert.setTimestamp(m.getTimestamp());
                    alert.setLevel(2);
                    return alert;
                });

        criticalAlerts.print();

        env.execute();
    }
}
