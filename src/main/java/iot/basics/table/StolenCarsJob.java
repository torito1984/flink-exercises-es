package iot.basics.table;

import iot.basics.async.perevent.AsyncEnrichmentFunction;
import iot.basics.entities.*;
import iot.basics.operators.JobUtils;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import java.util.concurrent.TimeUnit;
import org.apache.flink.types.Row;

import static iot.basics.db.VehicleOwnerDataClient.MAX_PARALLELISM;

public class StolenCarsJob {

    private static DataStream<EnrichedVehicleState> getVehicleState(StreamExecutionEnvironment env) {
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

        // Connect all the streams to get a potential state of the vehicle
        DataStream<VehicleState> vehicleState = measurements.connect(valveState).map(new CoMapFunction<SensorMeasurement, ValveState, VehicleState>() {
            @Override
            public VehicleState map1(SensorMeasurement m) throws Exception {
                return new VehicleState(m.getSensorId()/100, 1, m.getTimestamp());
            }

            @Override
            public VehicleState map2(ValveState v) throws Exception {
                return new VehicleState(v.getSensorId()/100, 1, v.getTimestamp());
            }
        });


        DataStream<EnrichedVehicleState> enrichedMeasurements = AsyncDataStream.unorderedWait(
                vehicleState,
                new AsyncEnrichmentFunction(), 50, TimeUnit.MILLISECONDS, MAX_PARALLELISM);

        return enrichedMeasurements;
    }

    public static void main(String [] args) throws Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = JobUtils.getEnv();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

        fsTableEnv.registerDataStream("vehicles_moving", getVehicleState(env));

        // The schema is inferred automatically by Flink
        //fsTableEnv.scan("vehicles_moving").printSchema();
        Table stolenCarCases = fsTableEnv.scan("vehicles_moving")
                .filter("owner.get('reportedStolen') == true")
                .select("state.get('vehicleId'), state.get('timestamp'), owner");

        fsTableEnv.toAppendStream(stolenCarCases, Row.class).print();

        env.execute();
    }
}
