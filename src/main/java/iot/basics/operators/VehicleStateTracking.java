package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.ValveState;
import iot.basics.entities.VehicleState;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Ejercicio 14: Monitorizar uso de los vehiculos
 *
 * Issue: Nos piden si seria posible aprovechar las medidas de todos los sensores del coche para saber patrones de uso
 * de los automobiles. Se considera que si los sensores esta midiendo actividad, significa que el automobil esta en uso.
 * Debido a que distintos sensores notifican en distintos momentos del tiempo, seria util pode utilizar todas las medidas
 * en conjunto para tener una traza lo mas fiable posible.
 *
 * Solucion: conectamos todos los streams con los que contamos en un colo stream y los transformamos a un api comun
 * que notifique el uso del automobil.
 *
 */
public class VehicleStateTracking {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv();


        // Ingestamos todos los steam disponibles

        // Medidas de temperatura
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

        // Medidas de estado de las valvulas
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

        // Conectamos todos los streams en uno unico
        DataStream<VehicleState> vehicleState = measurements.connect(valveState)
                // Comap nos permite tratar cada stream de manera diferente. Utilizamos esta capacidad para mapear ambos
                // streams a una API de actividad comun
                .map(new CoMapFunction<SensorMeasurement, ValveState, VehicleState>() {
                    @Override
                    public VehicleState map1(SensorMeasurement m) throws Exception {
                        return new VehicleState(m.getSensorId()/100, 1, m.getTimestamp());
                    }

                    @Override
                    public VehicleState map2(ValveState v) throws Exception {
                        return new VehicleState(v.getSensorId()/100, 1, v.getTimestamp());
                    }
                });

        vehicleState.print();

        env.execute();
    }
}