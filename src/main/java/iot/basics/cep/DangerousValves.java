package iot.basics.cep;

import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.entities.ValveState;
import iot.basics.operators.JobUtils;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.List;
import java.util.Map;

/**
 * Ejercicio 17: Errores complejos
 *
 * Issue: Nos solicitan que detectemos patrones de fallo mas complejos. En concreto, si una valvula se obstruye
 * (notifica OBSTRUCTED mas de 5 veces seguidas) y posteriormente la temperatura sube, se puede determinar que
 * la valvula esta en riesgo de rotura. Este tipo de alerta es de nivel 5.
 *
 * Solucion: utilizando la libreria de Complex Event Processing de Flink, podemos detectar esta secuencia de eventos
 * del stream conjunto de temperaturas y estados de valvulas. Si este patron se detecta para algun sensor, se notifica
 * la alerta de un nivel superior.
 *
 */
public class DangerousValves {

    // Stream de medidas de temperatura
    private static DataStream<Tuple2<SensorMeasurement, ValveState>> getMeasurementStream(StreamExecutionEnvironment env){
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

        // Stream de medidas de estado de valvulas
        DataStream<ValveState> valveState = env.addSource(new ValveStateSource(10000, 0.3))
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

        // Juntamos medidas de temperatura y valvula que esten proximas en el tiempo
        return measurements.join(valveState)
                .where(m ->m.getSensorId())
                .equalTo(v -> v.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply(new FlatJoinFunction<SensorMeasurement, ValveState, Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public void join(SensorMeasurement sensorMeasurement, ValveState valveState, Collector<Tuple2<SensorMeasurement, ValveState>> collector) throws Exception {
                        if((sensorMeasurement.getTimestamp() > valveState.getTimestamp()) && (sensorMeasurement.getTimestamp() - valveState.getTimestamp()) < 200){
                            collector.collect(new Tuple2<>(sensorMeasurement, valveState));
                        }
                    }
                });
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();

        DataStream<Tuple2<SensorMeasurement, ValveState>> fullState = getMeasurementStream(env);

        /**
         * Definimos el patron de error como:
         *  - comenzamos con un comportamiento normal
         *  - detectamos 5 medidas o mas en estado OBSTRUCTED
         *  - el patron temina con una deteccion de aumento de temperatura
         */

        Pattern<Tuple2<SensorMeasurement, ValveState>, ?> pattern = Pattern.<Tuple2<SensorMeasurement, ValveState>>
         begin("normal").where(
                new SimpleCondition<Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public boolean filter(Tuple2<SensorMeasurement, ValveState> event) {
                        return (event.f1.getValue() == SensorValue.State.OPEN) || (event.f1.getValue() == SensorValue.State.CLOSE);
                    }
                }
         ).next("closure").timesOrMore(5).greedy().where(
                new SimpleCondition<Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public boolean filter(Tuple2<SensorMeasurement, ValveState> event) {
                        return (event.f1.getValue() == SensorValue.State.CLOSE) || (event.f1.getValue() == SensorValue.State.OBSTRUCTED);
                    }
                }
        ).followedBy("heating").where(
                new SimpleCondition<Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public boolean filter(Tuple2<SensorMeasurement, ValveState> event) {
                        return event.f0.getValue() > 500;
                    }
                }
        );


        // Aplicamos el CEP al stream de medidas de sensor
        PatternStream<Tuple2<SensorMeasurement, ValveState>> patternStream = CEP.pattern(
                // Agrupamos el stream para cada sensor antes de aplicar el patron
                fullState.keyBy((KeySelector<Tuple2<SensorMeasurement, ValveState>, Long>) m -> m.f0.getSensorId()),
                pattern);

        // Una vez detectamos un evento que cuadra con el patron notificamos una alerta con el nivel adecaudo
        DataStream<SensorAlert> complexAlert = patternStream.process(
                new PatternProcessFunction<Tuple2<SensorMeasurement, ValveState>, SensorAlert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Tuple2<SensorMeasurement, ValveState>>> pattern,
                            Context ctx,
                            Collector<SensorAlert> out) throws Exception {
                        SensorAlert alert = new SensorAlert();
                        alert.setSensorId(pattern.get("normal").get(0).f0.getSensorId());
                        alert.setUnits("MALFUNCTIONING NOT OPENING TIMES " + pattern.get("closure").stream().count());
                        alert.setValue(pattern.get("heating").get(0).f0.getValue());
                        alert.setTimestamp(pattern.get("heating").get(0).f0.getTimestamp());
                        alert.setLevel(5);
                        out.collect(alert);
                    }
                });

        complexAlert.print();

        env.execute();
    }
}
