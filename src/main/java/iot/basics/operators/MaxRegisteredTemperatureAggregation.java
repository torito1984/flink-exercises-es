package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Ejercicio 7 (version 2): Maxima temperatura registrada para cada valvula.
 *
 * Issue: Nos reportan que es necesario registrar cual ha sido la mayor temperatura registrada para cada valvula. Si la
 * teperatura maxima es muy alta, puede afectar al funcionamiento futuro del automobil, incluso aunque la temperatira
 * sea soportada durante un periodo corto de tiempo.
 *
 * Solucion: En este caso resolvemos el mismo caso con la operacion maximo predefinida en Flink
 *
 */
public class MaxRegisteredTemperatureAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<SensorMeasurement> maxRegistered = measurements
                .keyBy("sensorId")
                .maxBy("value")
                .filter(m -> m.getSensorId() == 11080);

        maxRegistered.print();

        env.execute();
    }
}
