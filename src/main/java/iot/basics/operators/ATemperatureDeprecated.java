package iot.basics.operators;

import iot.basics.entities.MAValue;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Ejercicio 8: Calcular la temperatura media de uso
 *
 * Issue: nos piden reportar la temperatura media de uso de cada valvula.
 *
 * Solucion: mantener un agregado continuo de valores y numero de medidas. Cada vez que llegue un elemento, reportar
 * un nuevo valor medio. En caso de que este calculo sea mucha carga para cada medida recibida, se puede reducir el
 * numero de reportes por medida recibida.
 *
 * NOTA: Esta solucion ha sido descartada en la version 1.9 debido a que fold no es tan eficiente para la plataforma
 * como otros operadores. Ver el siguiente ejercicio para una solucion equivalente recomendada en las nuevas versiones
 * de Flink.
 */
public class ATemperatureDeprecated {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<Tuple3<Long, Long, Double>> maxRegistered = measurements
                .keyBy("sensorId")
                // La operacion fold comienza con un valor inicial (ver constructor de MAValue) y aplica el opeador
                // para el ultimo agregado y la nueva medida recibida
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
                // Transformamos el acumulado total en una media
                .map(new MapFunction<MAValue, Tuple3<Long, Long, Double>>() {
                     @Override
                     public Tuple3<Long, Long, Double> map(MAValue ma) throws Exception {
                         return new Tuple3<>(ma.getSensorId(), ma.getTimestamp(), ma.getCum() / ma.getCount());
                     }
                })
                // Observamos un solo sensor por simplicidad (quitar en produccion)
                .filter(m -> m.f0 == 11080);

        maxRegistered.print();

        env.execute();
    }
}
