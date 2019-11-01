package iot.basics.entities;

import java.io.Serializable;
import java.util.Objects;

public class MeanValveBehavior implements Serializable {
    private double pctOpen;
    private double pctClosed;
    private double pctObstructed;
    private double meanTemperature;

    public MeanValveBehavior(double pctOpen, double pctClosed, double pctObstructed, double meanTemperature) {
        this.pctOpen = pctOpen;
        this.pctClosed = pctClosed;
        this.pctObstructed = pctObstructed;
        this.meanTemperature = meanTemperature;
    }

    public double getPctOpen() {
        return pctOpen;
    }

    public void setPctOpen(double pctOpen) {
        this.pctOpen = pctOpen;
    }

    public double getPctClosed() {
        return pctClosed;
    }

    public void setPctClosed(double pctClosed) {
        this.pctClosed = pctClosed;
    }

    public double getMeanTemperature() {
        return meanTemperature;
    }

    public void setMeanTemperature(double meanTemperature) {
        this.meanTemperature = meanTemperature;
    }

    public double getPctObstructed() {
        return pctObstructed;
    }

    public void setPctObstructed(double pctObstructed) {
        this.pctObstructed = pctObstructed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeanValveBehavior that = (MeanValveBehavior) o;
        return Double.compare(that.pctOpen, pctOpen) == 0 &&
                Double.compare(that.pctClosed, pctClosed) == 0 &&
                Double.compare(that.pctObstructed, pctObstructed) == 0 &&
                Double.compare(that.meanTemperature, meanTemperature) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pctOpen, pctClosed, pctObstructed, meanTemperature);
    }

    @Override
    public String toString() {
        return "MeanValveBehavior{" +
                "pctOpen=" + pctOpen +
                ", pctClosed=" + pctClosed +
                ", pctObstructed=" + pctObstructed +
                ", meanTemperature=" + meanTemperature +
                '}';
    }
}
