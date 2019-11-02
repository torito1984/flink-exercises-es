package iot.basics.async;

import iot.basics.async.perevent.AsyncEnrichmentFunction;
import iot.basics.entities.EnrichedVehicleState;
import iot.basics.entities.SensorMeasurement;
import iot.basics.entities.ValveState;
import iot.basics.entities.VehicleState;
import iot.basics.operators.JobUtils;
import iot.basics.source.SensorMeasurementSource;
import iot.basics.source.ValveStateSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import java.util.concurrent.TimeUnit;
import static iot.basics.db.VehicleOwnerDataClient.MAX_PARALLELISM;


/**
 * Ejercicio 15: Anhadir informacion de propietario
 *
 * Issue: Nos piden que cada medida de actividad de los automobiles vaya acompañada de la informacion del dueño del
 * vehiculo. Esta informacion sera explotada en varios casos de uso futuros. Nos indican que si eliminamos las ultimas
 * 2 cifas significativas del id de sensor, obtenemos el id de vehiculo, que podemos cruzar con el CRM.
 *
 * Solucion: para cada medida nos conectamos a la base de datos de propietarios y encontramos el usuario adecuado. Juntamos
 * ambas fuentes de informacion y publicamos.
 *
 */
public class VehicleOwnerAsyncEnrichmentJob {

	/**
	 * Esta funcion genera el stream de actividad de vehiculos, ver ejercicio 14
	 */
	private static DataStream<VehicleState> getVehicleState(StreamExecutionEnvironment env) {
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

		return vehicleState;
	}


	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = JobUtils.getEnv();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Cargamos el stream de propietarios
		DataStream<VehicleState> vehicleState = getVehicleState(env);

		// Lo enriquecemos con informacion del CRM. Puesto que la llamda tiene cierto delay, lo hacemos de forma
		// asincrona. Recordad poner siempre un timeout para cada llamada y un maximo de llamadas en vuelo. Sino
		// es posible que el stream se bloquee
		DataStream<EnrichedVehicleState> enrichedMeasurements = AsyncDataStream.unorderedWait(
				vehicleState,
				new AsyncEnrichmentFunction(),
				50, TimeUnit.MILLISECONDS, MAX_PARALLELISM);

		enrichedMeasurements.print();

		env.execute();
	}

}
