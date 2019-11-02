package iot.basics.operators;

import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.entities.ValveState;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Ejercicio 13: Temperatura en cada estado
 *
 * Issue: Nos informan que las alarmas por temperatura deberian tener en cuenta el estado en el que se encontraba la valvula
 * en torno al momento de tomar la medicion. Las temperaturas de alarma son diferentes dependiendo del estado
 *  - OPEN-> mas de 500 grados
 *  - CLOSED -> mas de 550
 *  - OBSTRUCTED -> notificar alerta independientemente de la temperatura
 *
 *  Las alarmas de temperatura son de nivel 2, las alarmas de OBSTRUCCION son de nivel 3.
 *
 *  Solucion: Juntamos las medidas de estado de valvula y temperatura dentro de la misma ventana de 200 ms. Para cada
 *  par de temperatura y estado, si encontramos un par de medidas que cumplan el criterio, emitimos una alerta.
 *
 *  Separamos las alarmas de temperatura de aquellas que provienen de una obstruccion. De este modo podemos tratarlas
 *  independientemente, incluido el darles un nivel de alarma diferente.
 *
 */
public class StateTemperatureTracking {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();

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

        DataStream<ValveState> valveState = env.addSource(new ValveStateSource(10000, 0.01))
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

        // Join the two streams

        DataStream<Tuple2<SensorMeasurement, ValveState>> fullState = measurements.join(valveState)
                .where(m ->m.getSensorId())
                .equalTo(v -> v.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                .apply(new FlatJoinFunction<SensorMeasurement, ValveState, Tuple2<SensorMeasurement, ValveState>>() {
                    @Override
                    public void join(SensorMeasurement sensorMeasurement, ValveState valveState, Collector<Tuple2<SensorMeasurement, ValveState>> collector) throws Exception {
                        // Solo emitimos las medidas que esten proximas en el tiempo (<20msec), puesto que si estan mas lejos
                        // posiblemente el estado no se corresponde con la medida de temperatura
                        if((sensorMeasurement.getTimestamp() > valveState.getTimestamp()) && (sensorMeasurement.getTimestamp() - valveState.getTimestamp()) < 20){
                            collector.collect(new Tuple2<>(sensorMeasurement, valveState));
                        }
                    }
                });


        // De cara a tratar las alarmas de distinta naturaleza de manera diferente, las vamos a tratar como side
        // outputs diferentes
        final OutputTag<Tuple2<SensorMeasurement, ValveState>> outputOpenClosed =
                new OutputTag<Tuple2<SensorMeasurement, ValveState>>("open-closed"){};
        final OutputTag<Tuple2<SensorMeasurement, ValveState>> outputObstructed =
                new OutputTag<Tuple2<SensorMeasurement, ValveState>>("obstructed"){};

        SingleOutputStreamOperator<Tuple2<SensorMeasurement, ValveState>> alertsStream = fullState
            .filter(m -> (m.f0.getValue() > 500 && m.f1.getValue() == SensorValue.State.OPEN) ||
                         (m.f0.getValue() > 550 && m.f1.getValue() == SensorValue.State.CLOSE) ||
                         (m.f1.getValue() == SensorValue.State.OBSTRUCTED))
            .process(new ProcessFunction<Tuple2<SensorMeasurement, ValveState>, Tuple2<SensorMeasurement, ValveState>>() {
                @Override
                public void processElement(Tuple2<SensorMeasurement, ValveState> m, Context context,
                                           Collector<Tuple2<SensorMeasurement, ValveState>> collector) throws Exception {
                    switch(m.f1.getValue()){
                        case OPEN:
                        case CLOSE:
                            //Si es una alarma de temperatura, escribir al side output de alarmas de temperatura
                            context.output(outputOpenClosed, m);
                            break;
                        case OBSTRUCTED:
                            // Si es una alarma de obstruccion, escribir al side output de alarmas de obstruccion
                            context.output(outputObstructed, m);
                    }
                }
            });

        // Aqui caputuramos las alarmas de temperatura y emitimos de la manera indicada en la especificacion
        alertsStream.getSideOutput(outputOpenClosed).map( m -> {
            SensorAlert alert = new SensorAlert();
            alert.setSensorId(m.f0.getSensorId());
            alert.setUnits("MALFUNCTIONING TEMPERATURE");
            alert.setValue(m.f0.getValue());
            alert.setTimestamp(m.f0.getTimestamp());
            alert.setLevel(2); // El nivel de alarma de temperatura es 2
            return alert;
        }).print();

        // Aqui caputuramos las alarmas de obstuccion y emitimos de la manera indicada en la especificacion
        alertsStream.getSideOutput(outputOpenClosed).map( m -> {
            SensorAlert alert = new SensorAlert();
            alert.setSensorId(m.f0.getSensorId());
            alert.setUnits("MALFUNCTIONING OBSTRUCTED");
            alert.setValue(m.f0.getValue());
            alert.setTimestamp(m.f0.getTimestamp());
            alert.setLevel(3); // El nivel de alarma de obstruccion es 3
            return alert;
        }).print();


        env.execute();
    }
}