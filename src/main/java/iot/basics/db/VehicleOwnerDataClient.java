package iot.basics.db;

import iot.basics.entities.OwnerReferenceData;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class VehicleOwnerDataClient {

    private static final Random RAND = new Random(42);
    public static final  int    MAX_PARALLELISM = 30;

    private static ExecutorService pool = Executors.newFixedThreadPool(MAX_PARALLELISM,
            new ThreadFactory() {
                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = threadFactory.newThread(r);
                    thread.setName("temp-client-" + thread.getName());
                    return thread;
                }
            });


    public void asyncGetOwnerReferenceDataFor(long vehicleId, Consumer<OwnerReferenceData> callback) {
        CompletableFuture.supplyAsync(new OwnerReferenceDataSupplier(vehicleId), pool)
                .thenAcceptAsync(callback, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }

	private static class OwnerReferenceDataSupplier implements Supplier<OwnerReferenceData> {

	    private long vehicleId;

	    public OwnerReferenceDataSupplier(final long vehicleId) {
		    this.vehicleId = vehicleId;
	    }

	    @Override
        public OwnerReferenceData get() {
            try {
                Thread.sleep(RAND.nextInt(5) + 5);
            } catch (InterruptedException e) {
                //Swallowing Interruption here
            }
            return getRandomReferenceDataFor(vehicleId);
        }
	}

	private static List<String> fName =  Arrays.asList("Romeo", "Blake", "Aarav", "Cillian", "Lorenzo", "Aleksander", "Vincent", "Ted", "Elliot",
	"Ralphy", "Melissa", "Dolly", "Fern", "Hollie", "Evie", "Nancy", "Daniella", "Olivia-Rose", "Delilah", "Norah", "Jo", "Skylar", "Ali", "Kit", "Jackie", "Glenn", "Kai", "Alex", "Charlie", "Reggie");
	private static List<String> lName = Arrays.asList("Harvey", "Baker", "Saunders", "Green", "Houghton", "Kennedy", "Russell", "Wood", "Cole", "Kennedy", "Berry",
	"Day", "Mitchell", "Jackson", "Chambers", "Robertson", "Matthews", "Bailey", "Reynolds", "Lloyd", "Johnston", "Lloyd", "Bates", "Ward", "Hunter", "Harper", "Austin", "Kennedyv", "Fletcher", "Bates");

	public static OwnerReferenceData getRandomReferenceDataFor(long vehicleId) {
		return new OwnerReferenceData( fName.get((int) vehicleId % fName.size()) + " " + lName.get((int) vehicleId % lName.size()),
				RAND.nextDouble() > 0.95);
	}
}
