package iot.basics.async.perevent;

import iot.basics.db.VehicleOwnerDataClient;
import iot.basics.entities.EnrichedVehicleState;
import iot.basics.entities.OwnerReferenceData;
import iot.basics.entities.VehicleState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.Collections;
import java.util.function.Consumer;

/**
 * Funcion utilizada para conectarse al CRM y enriquecer el stream
 */
public class AsyncEnrichmentFunction extends RichAsyncFunction<VehicleState, EnrichedVehicleState> {

	private VehicleOwnerDataClient client;

	/**
	 * En la apertura, abrimos las conexiones necesarias
	 * @param parameters
	 * @throws Exception
	 */
	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		client = new VehicleOwnerDataClient();
	}

	/**
	 * En cada invocacion asincrona, usamos un futuro para retornar el valor enriquecido una vez que el CRM remoto responda
	 *
	 * @param vehicleState
	 * @param resultFuture
	 * @throws Exception
	 */
	@Override
	public void asyncInvoke(
			final VehicleState vehicleState,
			final ResultFuture<EnrichedVehicleState> resultFuture) throws Exception {

		client.asyncGetOwnerReferenceDataFor(
				vehicleState.getVehicleId(),
				new Consumer<OwnerReferenceData>() {
					@Override
					public void accept(final OwnerReferenceData owner) {
						resultFuture.complete(Collections.singletonList(new EnrichedVehicleState(vehicleState, owner)));
					}
				});

	}
}
