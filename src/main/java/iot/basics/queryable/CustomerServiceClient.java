package iot.basics.queryable;

import iot.basics.entities.CustomerReport;
import iot.basics.entities.SensorAlert;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.CompletableFuture;

public class CustomerServiceClient {

    public static void main(String[] args) throws Exception{
        QueryableStateClient client = new QueryableStateClient("127.0.1.1", 9069);
        client.setExecutionConfig(new ExecutionConfig());

        String what = args[0];
        long key = Long.parseLong(args[1]);
        JobID jobId = JobID.fromHexString(args[2]);

        ValueStateDescriptor<CustomerReport> complaintDescriptor =
                new ValueStateDescriptor<>(
                        "latestComplaint", // the state name
                        CustomerReport.class); // state class

        ValueStateDescriptor<SensorAlert> alertDescriptor =
                new ValueStateDescriptor<>(
                        "latestAlert", // the state name
                        SensorAlert.class); // state class


        if(what.equals("complaint")){
            CompletableFuture<ValueState<CustomerReport>> resultFuture =
                    client.getKvState(jobId, "complaints", key, BasicTypeInfo.LONG_TYPE_INFO, complaintDescriptor);
            System.out.println(resultFuture.get().value());
        }else if (what.equals("alert")){
            CompletableFuture<ValueState<SensorAlert>> resultAlert =
                    client.getKvState(jobId, "alerts", key, BasicTypeInfo.LONG_TYPE_INFO, alertDescriptor);
            System.out.println(resultAlert.get().value());
        }
    }
}
