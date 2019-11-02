package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Ejercicio 3: Transformar temperatura a Fahrenheit
 *
 * Issue: Nos informan que los sistemas de monitorizacion de materiales solo funcionan con temperaturas en Fahrenheit, sin embargo el
 * sistema IOT esta reportando en Celsius. Es preciso hacer la correccion antes de reportar valores.
 *
 * Solución: utilizar operadores de Flink
 */
public class ToFahrenheit extends JobUtils {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        // map toma cada valor del stream y le aplica una modificacion. El stream resultado en este ejemplo tiene las
        // temperaturas en la unidad correcta
        DataStream<SensorMeasurement> farenheit = measurements.map( m -> {
            m.setValue(m.getValue()*1.8 + 32);
            m.setUnits("FAHRENHEIT");
            return m;
        });

        farenheit.print();

        env.execute();
    }
}
