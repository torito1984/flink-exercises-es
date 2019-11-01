package iot.basics.operators;

import iot.basics.entities.SensorValue;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class DoubleSensorAverageAggregate implements AggregateFunction<SensorValue, Tuple4<Double, Double, Double, Double>,  Tuple4<Double, Double, Double, Double>> {

    private static final String PRIMARY = "PRIMARY";

    @Override
    public Tuple4<Double, Double, Double, Double> createAccumulator() {
        return new Tuple4<>(0.0, 0.0, 0.0, 0.0);
    }

    @Override
    public Tuple4<Double, Double, Double, Double> add(SensorValue i, Tuple4<Double, Double, Double, Double> acc) {
        if(i.getType().equals(PRIMARY)){
            return new Tuple4(acc.f0+1, acc.f1 + i.getValue(), acc.f2, acc.f3);
        }else{
            return new Tuple4(acc.f0, acc.f1, acc.f2 + 1, acc.f3 + i.getValue());
        }
    }

    @Override
    public Tuple4<Double, Double, Double, Double> getResult(Tuple4<Double, Double, Double, Double> acc) {
        return acc;
    }

    @Override
    public Tuple4<Double, Double, Double, Double> merge(Tuple4<Double, Double, Double, Double> acc1, Tuple4<Double, Double, Double, Double> acc2) {
        return new Tuple4<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2, acc1.f3 + acc2.f3);
    }
}