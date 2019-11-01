package iot.basics.source;


import iot.basics.entities.SecondarySensorMeasurement;

import java.util.SplittableRandom;

public class SecondarySensorMeasurementSource extends BaseGenerator<SecondarySensorMeasurement> {

	public static final long SENSOR_COUNT = 100_000L;

	public SecondarySensorMeasurementSource(final int maxRecordsPerSecond) {
		super(maxRecordsPerSecond);
	}

	@Override
	protected SecondarySensorMeasurement randomEvent(final SplittableRandom rnd, final long id) {
		return SecondarySensorMeasurement.builder().sensorId(rnd.nextLong(SENSOR_COUNT))
								.timestamp(System.currentTimeMillis() - rnd.nextLong(1000L))
								// Simulation of faulty measures
								.value(rnd.nextDouble(600) * (rnd.nextDouble() < 0.95 ? 1 : 10))
								.units("CELSIUS")
								.build();
	}
}
