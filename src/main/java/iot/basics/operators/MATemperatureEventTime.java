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
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
 * NOTA: En esta solucion utilizamos tiempo de evento para la construccion de ventanas. Debido a esto, las medidas
 * pueden llegar fuera de orden. En la generacion de watermarks, le damos un margen de 3.5 segundos para late events.
 * Este tiempo de margen tiene que ser determinado por experimentacion para cada caso de uso.
 */
public class MATemperatureEventTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();

        // Vamos a utilizar tiempo de evento para que las medidas reportadas coincidan con el tiempo percibido por el usuario
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));

        // Asignamos un watermark de progreso al stream de medidas del sensor.
        DataStream<SensorMeasurement> withWatermarks = measurements.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorMeasurement>() {
            // Este el el tiempo maximo que permitimos para que una medida tardia se considere dentro de su ventana
            private final long maxOutOfOrderness = 3500; // 3.5 seconds
            // El ultimo timestamp observado
            private long currentMaxTimestamp;

            // Cada vez que llega un elemento, extraemos la medida de tiempo que lleva consigo
            public long extractTimestamp(SensorMeasurement sensorMeasurement, long previousElementTimestamp) {
                long timestamp = sensorMeasurement.getTimestamp();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }

            // Cada vez que se requiera generar un watermark (la periodicidad de esto se configura a nivel de job),
            // esta funcion reporta el valor que ha de considerarse
            public Watermark getCurrentWatermark() {
                // return the watermark: el mayor timestamp observado menos el margen elegido para eventos tardios
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
        });

        // Que ocurre si no asigno watemarks al stream? Puedes probarlo tomando measurements como el stream base de este
        // job. Deberias observar que, sin watermarks, el tiempo no pasa para el job, las ventanas no se ejecutan nunca,
        // y los valores se acumulan en memoria sin ser reportados nunca. Con tiempo de evento es necesario asignar watermarks
        // para que cualquier mecanismo de ventanas temporales funcione.
        //DataStream<SensorMeasurement> useMeasurements = measurements;
        DataStream<SensorMeasurement> useMeasurements = withWatermarks;

        // Este calculo es el mismo que en el ejercicio anterior. La unica diferencia es que el tiempo reportado y la
        // asignacion a ventanas temporales corresponde al tiempo de evento reportado por los sensores en origen
        DataStream<Tuple3<Long, Long, Double>> meanRegistered = useMeasurements
                .keyBy("sensorId")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
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
                }, new ProcessWindowFunction<MAValue, Tuple3<Long, Long, Double>, Tuple, TimeWindow>() {
                    public void process(Tuple tuple, Context context, Iterable<MAValue> iterable, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
                        MAValue value = iterable.iterator().next();
                        collector.collect(new Tuple3<>(value.getSensorId(), value.getTimestamp(), value.getCum()/value.getCount()));
                    }
                })
                // Observamos solo un sensor por simplicidad
                .filter(m -> m.f0 == 11080);

        meanRegistered.print();

        env.execute();
    }
}
