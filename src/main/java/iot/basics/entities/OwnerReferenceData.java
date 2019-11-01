package iot.basics.entities;

import java.util.Objects;

public class OwnerReferenceData {
    private String ownerName;
    private boolean reportedStolen;

    public OwnerReferenceData() {
    }

    public OwnerReferenceData(String ownerName, boolean reportedStolen) {
        this.ownerName = ownerName;
        this.reportedStolen = reportedStolen;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public boolean isReportedStolen() {
        return reportedStolen;
    }

    public void setReportedStolen(boolean reportedStolen) {
        this.reportedStolen = reportedStolen;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OwnerReferenceData that = (OwnerReferenceData) o;
        return reportedStolen == that.reportedStolen &&
                Objects.equals(ownerName, that.ownerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ownerName, reportedStolen);
    }

    @Override
    public String toString() {
        return "OwnerReferenceData{" +
                "ownerName='" + ownerName + '\'' +
                ", reportedStolen=" + reportedStolen +
                '}';
    }
}
