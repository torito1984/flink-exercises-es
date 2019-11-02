package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Ejercicio 2: Lectura de señales procedentes del sensor de temperatura.
 *
 * En este ejercicio unicamente nos conectamos a la fuente de datos IOT de todos los sensores de temperatura desplegados
 * e imprimimos sus valores para observar la estructura que tienen. La estructura del programa es exactamente igual
 * a la del ejercicio 1 (ver FlinkHelloWorld)
 */
public class BasicPrintJob extends JobUtils {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = JobUtils.getEnv();

		// Leer fuente de datos
		DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

		// Imprimir
		measurements.print();

		// Ejecutar
		env.execute();
	}
}
