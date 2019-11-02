package iot.basics.operators;

import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 *  Ejercicio 5: Alertas para valvulas con temperaturas altas
 *
 *  Issue: Nos reportan que  es necesario saber que valvulas tienen una temperatura mayor a 550 grados, puesto que esto
 *  puede ser peligroso. Esta alerta seria menor, de nivel 2.
 *
 *  Solución: Filtrar aquellas medidas que muestran un valor mayor a 550 grados y reportar un valor de alerta
 */
public class HeatAlert {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<SensorAlert> criticalAlerts = measurements
                // Filtramos los valores muy grandes
                .filter(m -> m.getValue() > 550)
                // Si encontramos alguno reportamos una alerta
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
