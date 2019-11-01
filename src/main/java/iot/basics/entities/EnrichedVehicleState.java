package iot.basics.entities;

import java.util.Objects;

public class EnrichedVehicleState {

    private VehicleState state;
    private OwnerReferenceData owner;

    public EnrichedVehicleState() {
    }

    public EnrichedVehicleState(VehicleState state, OwnerReferenceData owner) {
        this.state = state;
        this.owner = owner;
    }

    public VehicleState getState() {
        return state;
    }

    public void setState(VehicleState state) {
        this.state = state;
    }

    public OwnerReferenceData getOwner() {
        return owner;
    }

    public void setOwner(OwnerReferenceData owner) {
        this.owner = owner;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedVehicleState that = (EnrichedVehicleState) o;
        return Objects.equals(state, that.state) &&
                Objects.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, owner);
    }

    @Override
    public String toString() {
        return "EnrichedVehicleState{" +
                "state=" + state +
                ", owner=" + owner +
                '}';
    }
}
