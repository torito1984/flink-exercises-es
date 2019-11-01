package iot.basics.source;


import iot.basics.entities.SensorMeasurement;

import java.util.SplittableRandom;

public class SensorMeasurementSource extends BaseGenerator<SensorMeasurement> {

	public static final long SENSOR_COUNT = 100_000L;

	public SensorMeasurementSource(final int maxRecordsPerSecond) {
		super(maxRecordsPerSecond);
	}

	@Override
	protected SensorMeasurement randomEvent(final SplittableRandom rnd, final long id) {
		return SensorMeasurement.builder().sensorId(rnd.nextLong(SENSOR_COUNT))
								.timestamp(System.currentTimeMillis() - rnd.nextLong(1000L))
								.value(rnd.nextDouble(600))
								.units("CELSIUS")
								.build();
	}
}
