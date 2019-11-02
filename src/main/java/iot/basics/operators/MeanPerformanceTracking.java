package iot.basics.operators;

import iot.basics.entities.*;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.Iterator;


/**
 * Ejercicio 12: Historico de perfomance de cada valvula
 *
 * Issue: Nos piden que acumulemos tanto la temperatura media, como el porcentaje de cada estado en cada valvula.
 * Durante el funcionamiento, las valvulas pueden estar en estado OPEN, CLOSE u OBSTRUCTED. Se necesita saber que el porcentaje
 * de OPEN y CLOSE esta en equilibrio - mostrando un funcionamiento normal - y que el porcentaje del tiempo en OBSTRUCTED es
 * nulo o muy bajo.
 *
 */
public class MeanPerformanceTracking {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();


        // Ingestaos las medidas de temperatura
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorMeasurement>() {
                    private final long maxOutOfOrderness = 3500; // 3.5 seconds
                    private long currentMaxTimestamp;

                    public long extractTimestamp(SensorMeasurement sensorMeasurement, long previousElementTimestamp) {
                        long timestamp = sensorMeasurement.getTimestamp();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        // Ingestamos las medidas de estado de valvula
        DataStream<ValveState> valveState = env.addSource(new ValveStateSource(100000, 0.01))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ValveState>() {
                    private final long maxOutOfOrderness = 3500; // 3.5 seconds
                    private long currentMaxTimestamp;

                    public long extractTimestamp(ValveState sensorMeasurement, long previousElementTimestamp) {
                        long timestamp = sensorMeasurement.getTimestamp();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        // Juntamos ambos streams flyyendo en paralelo
        DataStream<MeanValveBehavior> meanPerformance = measurements.coGroup(valveState)
                // Agrupamos las medidas para cada sensor
                .where(m ->m.getSensorId())
                .equalTo(v -> v.getSensorId())
                // Revisamos en ventanas de 25 segundos
                .window(TumblingEventTimeWindows.of(Time.seconds(25)))
                // Aplicando una funcion de cogroup
                .apply(new CoGroupFunction<SensorMeasurement, ValveState, MeanValveBehavior>() {

                    /**
                     * Como se puede observar, coGroup nos da dos iteradores independientes para los elementos que entran
                     * dentro de la ventana temporal para ambos streams
                     */
                    @Override
                    public void coGroup(Iterable<SensorMeasurement> measurements, Iterable<ValveState> states, Collector<MeanValveBehavior> collector) throws Exception {
                        int total = 0;
                        int totaltmp = 0;
                        double open = 0.0;
                        double closed = 0.0;
                        double obstructed = 0.0;
                        double temperature = 0.0;

                        // Calculo de estado medio
                        Iterator<ValveState> it = states.iterator();
                        while (it.hasNext()){
                            total++;
                            switch (it.next().getValue()){
                                case OPEN:
                                    open++;
                                    break;
                                case CLOSE:
                                    closed++;
                                    break;
                                case OBSTRUCTED:
                                    obstructed++;
                                    break;
                            }
                        }

                        // Calculo de temepatura media
                        Iterator<SensorMeasurement> its = measurements.iterator();
                        while (its.hasNext()){
                            totaltmp++;
                            temperature += its.next().getValue();
                        }

                        // Emitir el performance medio
                        if(total > 0 && totaltmp > 0)
                            collector.collect(new MeanValveBehavior(open/total, closed/total,
                                obstructed/total,temperature/totaltmp));

                    }
                });

        meanPerformance.print();

        env.execute();
    }
}