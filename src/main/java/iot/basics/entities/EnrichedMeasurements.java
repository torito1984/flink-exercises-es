package iot.basics.entities;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichedMeasurements {

	private SensorMeasurement sensorMeasurement;
	private SensorReferenceData sensorReferenceData;
}
