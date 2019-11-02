package iot.basics.operators;

import iot.basics.entities.SecondarySensorMeasurement;
import iot.basics.entities.SensorAlert;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.SensorValue;
import iot.basics.source.SecondarySensorMeasurementSource;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.api.java.tuple.Tuple;
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

import java.util.Iterator;

/**
 * Ejercicio 10: Sensor secundario por seguirdad
 *
 * Issue: Nos comunican que, en los nuevos modelos, las valvulas llevan 2 sensores por seguridad. Nos ordenan comparar
 * las medidas recibidas por ambos sensores. En el caso que la desviacion sea mayor a 20 grados, reportar un error
 * de medida.
 *
 * Solucion: ingestar los dos streams de datos, juntarlos por el id de sensor y comparar las medidas recibidas en torno
 * al mismo momento en el tiempo. Si la desviacion es muy grande, reportar un error.
 *
 * NOTA: Esta solucion es correcta, pero se puede hacer mas eficiente. Recordar que al utilizar el API de bajo nivel
 * process, se acumulan todos los valores de cada ventana hasta que esta se ejecuta. Ver el siguiente ejercicio para
 * una solucion mas eficiente.
 *
 */
public class SecondarySensor {

    public static void main(String[] args) throws Exception {

        final int MAX_TOLERANCE = 20;

        StreamExecutionEnvironment env = JobUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Ingestamos el stream de medidas primario
        DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));
        // Ingestamos el stream de medidas secundario
        DataStream<SecondarySensorMeasurement> secondaryMeasurements = env.addSource(new SecondarySensorMeasurementSource(100_000));

        // Convertimos las medidas de ambos sensores a un mismo API, de este modo podremos unir ambas medidas en el
        // mismo stream. Marcamos si la medida proviene de un sensor primario o secundario
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

        // Unimos ambos streams. De este modo, es transparente la procedencia de la medida
        DataStream<SensorValue> allValues = primaryValues.union(secondaryValues)
          // le asignamos al stream un watermark
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
          // Agrupamos las medidas recibidas para cada sensor en ventamas de 10 segundos
          .keyBy("sensorId")
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          // para cada ventana, calculamos la temperatura media del sensor primario y secundario, en el caso de encontrar
          // una desviacion significativa, reportamos una alerta
          .process(new ProcessWindowFunction<SensorValue, SensorAlert, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<SensorValue> iterable, Collector<SensorAlert> collector) throws Exception {
                double countPrimary = 0;
                double sumPrimary = 0;
                double countSecondary = 0;
                double sumSecondary = 0;

                long sensorId = 0;

                Iterator<SensorValue> it = iterable.iterator();
                while(it.hasNext()){
                    SensorValue i = it.next();
                    sensorId = i.getSensorId();
                    if(i.getType().equals("PRIMARY")){
                        countPrimary++;
                        sumPrimary += i.getValue();
                    }else{
                        countSecondary++;
                        sumSecondary += i.getValue();
                    }
                }

                double sensorDiff = Math.abs(sumPrimary/countPrimary - sumSecondary/countSecondary);

                // Si la diferencia es muy grande, reportar una alerta
                if(sensorDiff > MAX_TOLERANCE){
                    SensorAlert alert = new SensorAlert();
                    alert.setTimestamp(context.window().getEnd());
                    alert.setSensorId(sensorId);
                    alert.setUnits("SENSOR DIFF CELSIUS");
                    alert.setValue(sensorDiff);
                    alert.setLevel(2);
                    collector.collect(alert);
                }
            }
        });

        sensorAlerts.print();
        env.execute();


    }
}
