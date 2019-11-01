package iot.basics.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class SensorIdPartitioner implements Partitioner<Long> {
    @Override
    public int partition(final Long sensorMeasurement, final int numPartitions) {
        return Math.toIntExact(sensorMeasurement % numPartitions);
    }
}