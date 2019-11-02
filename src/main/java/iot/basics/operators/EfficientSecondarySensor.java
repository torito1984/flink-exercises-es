package iot.basics.operators;

import iot.basics.entities.SecondarySensorMeasurement;
import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.source.SecondarySensorMeasurementSource;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * Ejercicio 11: Sensor secundario por seguirdad
 *
 * Issue: Nos comunican que, en los nuevos modelos, las valvulas llevan 2 sensores por seguridad. Nos ordenan comparar
 * las medidas recibidas por ambos sensores. En el caso que la desviacion sea mayor a 20 grados, reportar un error
 * de medida.
 *
 * Solucion: ingestar los dos streams de datos, juntarlos por el id de sensor y comparar las medidas recibidas en torno
 * al mismo momento en el tiempo. Si la desviacion es muy grande, reportar un error.
 *
 * NOTA: Esta solucion es correcta y mas eficiente, gracias a la utilizacion de un agregador antes de aplicar la funcion
 * de ejecucion de ventana.
 *
 */
public class EfficientSecondarySensor {


    public static void main(String[] args) throws Exception {

        final int MAX_TOLERANCE = 20;

        StreamExecutionEnvironment env = JobUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100000));
        DataStream<SecondarySensorMeasurement> secondaryMeasurements = env.addSource(new SecondarySensorMeasurementSource(100000));

        // Map to a common API
        DataStream<SensorValue> primaryValues = measurements.map(m -> SensorValue.builder().sensorId(m.getSensorId())
                .timestamp(m.getTimestamp())
                .value(m.getValue())
                .units(m.getUnits())
                .type("PRIMARY")
                .build());

        DataStream<SensorValue> secondaryValues = secondaryMeasurements.map(m -> SensorValue.builder().sensorId(m.getSensorId())
                .timestamp(m.getTimestamp())
                .value(m.getValue())
                .units(m.getUnits())
                .type("SECONDARY")
                .build());

        // Union the streams and assign watermarks
        DataStream<SensorValue> allValues = primaryValues.union(secondaryValues)
          .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorValue>() {
            private final long maxOutOfOrderness = 3500; // 3.5 seconds
            private long currentMaxTimestamp;

            public long extractTimestamp(SensorValue sensorMeasurement, long previousElementTimestamp) {
                long timestamp = sensorMeasurement.getTimestamp();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }

            public Watermark getCurrentWatermark() {
                // return the watermark as current highest timestamp minus the out-of-orderness bound
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
          });

        DataStream<SensorAlert> sensorAlerts = allValues
            .keyBy("sensorId")
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
             // En esta solucion, usamos un agregador que mantiene el total para ambos sensores actualizado hasta el momento
             // de ejecutar la ventana. Ver el codigo de DoubleSensorAverageAggregate para ver como se realiza el calculo
            .aggregate(new DoubleSensorAverageAggregate()
                    , new ProcessWindowFunction<Tuple4<Double, Double, Double, Double>, SensorAlert, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple tuple, Context context, Iterable<Tuple4<Double, Double, Double, Double>> iterable, Collector<SensorAlert> collector) throws Exception {
                    Tuple4<Double, Double, Double, Double> agg = iterable.iterator().next();
                    long sensorId = tuple.getField(0);
                    if(agg.f0 > 0 && agg.f2 > 0) {
                        double sensorDiff = Math.abs(agg.f1 / agg.f0 - agg.f3 / agg.f2);
                        if (sensorDiff > MAX_TOLERANCE){
                            SensorAlert alert = new SensorAlert();
                            alert.setTimestamp(context.window().getEnd());
                            alert.setSensorId(sensorId);
                            alert.setUnits("SENSOR DIFF CELSIUS");
                            alert.setValue(sensorDiff);
                            alert.setLevel(2);
                            collector.collect(alert);
                        }
                    }
                }
            });

        sensorAlerts.print();
        env.execute();
    }
}
