package iot.basics.source;


import iot.basics.entities.SensorValue;
import iot.basics.entities.ValveState;

import java.util.SplittableRandom;

public class ValveStateSource extends BaseGenerator<ValveState> {

	public static final long SENSOR_COUNT = 100_000L;
	private double probObs;

	public ValveStateSource(final int maxRecordsPerSecond,  double obsProb) {
		super(maxRecordsPerSecond);
		this.probObs = obsProb;
	}

	@Override
	protected ValveState randomEvent(SplittableRandom rnd, long id) {
		double random = rnd.nextDouble();
		SensorValue.State nexState = null;
		if(random < probObs){
			nexState = SensorValue.State.OBSTRUCTED;
		}else if((random > probObs) && random < (probObs + ((1-probObs)/2))){
			nexState = SensorValue.State.OPEN;
		}else{
			nexState = SensorValue.State.CLOSE;
		}

		return ValveState.builder().sensorId(rnd.nextLong(SENSOR_COUNT))
				.timestamp(System.currentTimeMillis() - rnd.nextLong(1000L))
				.value(nexState)
				.build();
	}
}
