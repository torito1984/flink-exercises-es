package iot.basics.operators;

import iot.basics.entities.MAValue;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Ejercicio 8: Calcular la temperatura media de uso
 *
 * Issue: nos piden reportar la temperatura media de uso de cada valvula.
 *
 * Solucion: mantener un agregado continuo de valores y numero de medidas. Cada vez que llegue un elemento, reportar
 * un nuevo valor medio. En caso de que este calculo sea mucha carga para cada medida recibida, se puede reducir el
 * numero de reportes por medida recibida.
 *
 * NOTA: esta solucion es mas recomendada en las nuevas versiones de Flink por eficiencia
 */
public class ATemperature {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        DataStream<Tuple3<Long, Long, Double>> meanRegistered = measurements
                .keyBy("sensorId")
                // Agrupamos todas las medidas en una ventana
                .windowAll(GlobalWindows.create())
                // Emitimos un valor para la ventana de medidas cada vez que llegue un elemento
                .trigger(CountTrigger.of(1))
                // Agregamos el total y el valor acumulado global
                .aggregate(new AggregateFunction<SensorMeasurement, MAValue, MAValue>() {
                    public MAValue createAccumulator() {
                        return new MAValue();
                    }

                    public MAValue add(SensorMeasurement o, MAValue previous) {
                        previous.setSensorId(o.getSensorId());
                        previous.setTimestamp((o.getTimestamp() > previous.getTimestamp()) ? o.getTimestamp() : previous.getTimestamp());
                        previous.setCount(previous.getCount() + 1);
                        previous.setCum(previous.getCum() + o.getValue());
                        return previous;
                    }

                    public MAValue getResult(MAValue emaValue) {
                        return emaValue;
                    }

                    public MAValue merge(MAValue a, MAValue b) {
                        MAValue out = new MAValue();
                        out.setSensorId(a.getSensorId());
                        out.setTimestamp((a.getTimestamp() < b.getTimestamp()) ? b.getTimestamp() : a.getTimestamp());
                        out.setCount(a.getCount() + b.getCount());
                        out.setCum(a.getCum() + b.getCum());
                        return out;
                    }
                }
                // Cada vez que se ejecute la ventana, procesamos el acumulado para generar la media global
                , new ProcessAllWindowFunction<MAValue, Tuple3<Long, Long, Double>, GlobalWindow>() {
                    public void process(Context context, Iterable<MAValue> iterable, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
                        MAValue value = iterable.iterator().next();
                        collector.collect(new Tuple3<>(value.getSensorId(), value.getTimestamp(), value.getCum()/value.getCount()));
                    }
                })
                // Filtramos el valor de un sensor por simplicidad (quitar en produccion)
                .filter(m -> m.f0 == 11080);

        meanRegistered.print();

        env.execute();
    }
}
