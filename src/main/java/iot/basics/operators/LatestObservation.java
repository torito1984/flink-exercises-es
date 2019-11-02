package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Ejercicio 6: Ultima medida tomada para cada coche.
 *
 * Issue: Nos reportan que seria util saber en todo momento cuando fue la ultima medida observada para cada coche. Esto
 * podria ayudar a saber si un coche no se usa durante mucho tiempo y notificar al usuario que esto puede traer problemas
 * en el futuro.
 *
 * Solución: Monitorizar el ultimo momento que hemos recibido medidas para cada automobil.
 *
 */
public class LatestObservation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));


        DataStream<SensorMeasurement> latestObservation = measurements
                // Agrupamos las medidas por sensor
                .keyBy("sensorId")
                // Monitorizamos la mayor fecha de medida recibida
                .maxBy("timestamp")
                // Observamos un sensor en concreto por simplicidad (TODO: quitar en produccion)
                .filter(m -> m.getSensorId() == 11080);

        latestObservation.print();

        env.execute();
    }
}
