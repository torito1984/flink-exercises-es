package iot.basics.operators;

import iot.basics.entities.MAValue;
import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * Ejercicio 9: Media mobil de la temperatura de uso
 *
 * Issue: Nos piden acumular la temperatura media de uso cada 10 segundos, con el objetivo de observar los distintos
 * patrones de utilizacion de la valvulas.
 *
 * Solucion: Construir ventanas de tiempo y calcular la media dentro de cada ventana.
 *
 * NOTA: En esta solucion utilizamos el tiempo de proceso, es decir, el momento en el que cada medida es procesada por
 * Flink. Si las medidas no llegan de inmediato al sistema (cosa poco probable puesto que los sensores emiten en remoto)
 * el tiempo reportado por Flink para cada media estara desalineado con el tiempo de uso que el usuario percibe de su
 * coche. Ver siguiente ejercicio para un manejo correcto del tiempo de evento.
 */
public class MATemperatureProcessingTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        // utilizamos tiempo de proceso para la construccion de las ventanas
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple3<Long, Long, Double>> meanRegistered = measurements
                .keyBy("sensorId")
                // Construimos ventanas cada 10 segundos, moviendose cada 5 segundos
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // Agregamos el valor total para cada ventana
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
                // Cada vez que se ejecuta una ventana, generamos el valor medio
                }, new ProcessWindowFunction<MAValue, Tuple3<Long, Long, Double>, Tuple, TimeWindow>() {
                    public void process(Tuple tuple, Context context, Iterable<MAValue> iterable, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
                        MAValue value = iterable.iterator().next();
                        collector.collect(new Tuple3<>(value.getSensorId(), value.getTimestamp(), value.getCum()/value.getCount()));
                    }
                })
                // filtramos solo un sensor por simplicidad de observacion (quitar en produccion)
                .filter(m -> m.f0 == 11080);

        meanRegistered.print();

        env.execute();
    }
}
