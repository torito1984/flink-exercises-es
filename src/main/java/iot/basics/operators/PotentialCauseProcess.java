package iot.basics.operators;

import iot.basics.entities.CustomerReport;
import iot.basics.entities.SensorAlert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Para intentar determinar una causa de error, mantenemos la ultima queja de propietario y la ultima alerta del vehiculo.
 * En el caso de detectar un caso en el que notificacion y alerta esta poximas en el tiempo notificamos una potencial
 * causa de la queja.
 */
public class PotentialCauseProcess extends KeyedCoProcessFunction<Long, CustomerReport, SensorAlert, Tuple2<CustomerReport, SensorAlert>> {

    private transient ValueState<CustomerReport> latestComplaint;
    private transient ValueState<SensorAlert>    latestAlert;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<CustomerReport> complaintDescriptor =
                new ValueStateDescriptor<>(
                        "latestComplaint", // the state name
                        CustomerReport.class); // state class
        latestComplaint = getRuntimeContext().getState(complaintDescriptor);

        ValueStateDescriptor<SensorAlert> alertDescriptor =
                new ValueStateDescriptor<>(
                        "latestAlert", // the state name
                        SensorAlert.class); // state class
        latestAlert = getRuntimeContext().getState(alertDescriptor);
    }

    @Override
    public void processElement1(CustomerReport customerReport, Context context, Collector<Tuple2<CustomerReport, SensorAlert>> collector) throws Exception {
        // access the state value
        if(customerReport.getValue().equals("BROKEN_DOWN") || customerReport.getValue().equals("DISSATISFIED")) {
            latestComplaint.update(customerReport);
            SensorAlert latestAlertSeen = latestAlert.value();

            if((latestAlertSeen != null) && Math.abs(latestAlertSeen.getTimestamp() - customerReport.getTimestamp()) < Time.hours(2).toMilliseconds()){
                collector.collect(new Tuple2<>(customerReport, latestAlertSeen));
            }
        }
    }

    @Override
    public void processElement2(SensorAlert sensorAlert, Context context, Collector<Tuple2<CustomerReport, SensorAlert>> collector) throws Exception {
        latestAlert.update(sensorAlert);

        CustomerReport latestComplaintSeen = latestComplaint.value();

        if((latestComplaintSeen != null) && Math.abs(sensorAlert.getTimestamp() - latestComplaintSeen.getTimestamp()) < Time.hours(2).toMilliseconds()){
            collector.collect(new Tuple2<>(latestComplaintSeen, sensorAlert));
        }
    }
}