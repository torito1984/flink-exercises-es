package iot.basics.entities;

import java.util.Objects;

public class VehicleState {
    private long vehicleId;
    private int moving;
    private long timestamp;

    public VehicleState() {
    }

    public VehicleState(long vehicleId, int moving, long timestamp) {
        this.vehicleId = vehicleId;
        this.moving = moving;
        this.timestamp = timestamp;
    }

    public long getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(long vehicleId) {
        this.vehicleId = vehicleId;
    }

    public int getMoving() {
        return moving;
    }

    public void setMoving(int moving) {
        this.moving = moving;
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
        VehicleState that = (VehicleState) o;
        return vehicleId == that.vehicleId &&
                moving == that.moving &&
                timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vehicleId, moving, timestamp);
    }

    @Override
    public String toString() {
        return "VehicleState{" +
                "vehicleId=" + vehicleId +
                ", moving=" + moving +
                ", timestamp=" + timestamp +
                '}';
    }
}
