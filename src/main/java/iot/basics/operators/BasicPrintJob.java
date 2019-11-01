package iot.basics.operators;

import iot.basics.entities.SensorMeasurement;
import iot.basics.source.SensorMeasurementSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class BasicPrintJob extends JobUtils {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = JobUtils.getEnv();
		DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(100_000));
		measurements.print();
		env.execute();
	}

	/*
	iterate
	custom partition
	*/

}
