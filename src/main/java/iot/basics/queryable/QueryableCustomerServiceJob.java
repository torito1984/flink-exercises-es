package iot.basics.queryable;

import iot.basics.entities.*;
import iot.basics.operators.JobUtils;
import iot.basics.source.ClientCommunicationSource;
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
import org.apache.flink.streaming.api.TimeCharacteristic;
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
 * Ejercicio 20: Consulta de ultima alarma y ultima queja registrada
 *
 * Issue: Desde el departamento de atencion al cliente nos piden poder consultar la ultima queja de propietario
 * o ultima alarma de telemetria registrada por cada uno de los vehiculos.
 *
 * Solucion: Partiendo de la solucion del ejercicio 16, ya tenemos resuelto este caso de uso. El unico cambio
 * que es necesario introducir el el ProcessFuncion es hacer el estado para cada vehiculo abierto a consulta desde
 * fuera del pipeline. Para ello utilizamos la capacidad de queryable state de Flink >1.9
 *
 */
public class QueryableCustomerServiceJob {

    // Stream de medidas
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

        // Join the two streams
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

    // Stream de alertas
    private static DataStream<SensorAlert> getAlerts(StreamExecutionEnvironment env) {
        DataStream<Tuple2<SensorMeasurement, ValveState>> fullState = getMeasurementStream(env);

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

        PatternStream<Tuple2<SensorMeasurement, ValveState>> patternStream = CEP.pattern(fullState
                        .keyBy((KeySelector<Tuple2<SensorMeasurement, ValveState>, Long>) m -> m.f0.getSensorId()),
                pattern);

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

        return complexAlert;
    }

    // stream de notificaciones de propietario
    private static DataStream<CustomerReport> getCustomerReports(StreamExecutionEnvironment env) {
        DataStream<CustomerReport> reports = env.addSource(new ClientCommunicationSource(1000, 0.05))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CustomerReport>() {
                    private final long maxOutOfOrderness = 3500; // 3.5 seconds
                    private long currentMaxTimestamp;

                    public long extractTimestamp(CustomerReport sensorMeasurement, long previousElementTimestamp) {
                        long timestamp = sensorMeasurement.getTimestamp();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    public Watermark getCurrentWatermark() {
                        // return the watermark as current highest timestamp minus the out-of-orderness bound
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        return reports;
    }

    public static void main(String [] args) throws Exception{

        StreamExecutionEnvironment env = JobUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CustomerReport> customerReports = getCustomerReports(env).keyBy(CustomerReport::getVehicleId);
        DataStream<SensorAlert> alerts = getAlerts(env).keyBy(new KeySelector<SensorAlert, Long>() {
            @Override
            public Long getKey(SensorAlert sensorAlert) throws Exception {
                return sensorAlert.getSensorId()/100;
            }
        });

        // permitimos que el estado del pipeline sea consultable desde el exterior.
        // Ver la funcion open de QueryablePotentialCauseProcess y como consultar desde un programa externo en QueryablePotentialCauseProcess
        DataStream<Tuple2<CustomerReport, SensorAlert>> potentialCauses = customerReports.connect(alerts)
                .process(new QueryablePotentialCauseProcess());


        potentialCauses.print();
        env.execute();
    }
}
