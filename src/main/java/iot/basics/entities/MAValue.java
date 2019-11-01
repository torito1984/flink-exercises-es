package iot.basics.entities;

import java.io.Serializable;
import java.util.Objects;

public class MAValue implements Serializable {
    private long sensorId;
    private long timestamp;
    private double count;
    private double cum;

    public MAValue() {
        this.sensorId = 0;
        this.timestamp = 0;
    }

    public long getSensorId() {
        return sensorId;
    }

    public void setSensorId(long sensorId) {
        this.sensorId = sensorId;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getCum() {
        return cum;
    }

    public void setCum(double cum) {
        this.cum = cum;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MAValue emaValue = (MAValue) o;
        return sensorId == emaValue.sensorId &&
                timestamp == emaValue.timestamp &&
                Double.compare(emaValue.count, count) == 0 &&
                Double.compare(emaValue.cum, cum) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, timestamp, count, cum);
    }

    @Override
    public String toString() {
        return "MAValue{" +
                "sensorId=" + sensorId +
                ", timestamp=" + timestamp +
                ", count=" + count +
                ", cum=" + cum +
                '}';
    }
}
