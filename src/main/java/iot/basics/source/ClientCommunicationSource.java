package iot.basics.source;


import iot.basics.entities.CustomerReport;
import iot.basics.entities.ValveState;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

public class ClientCommunicationSource extends BaseGenerator<CustomerReport> {

	public static final long SENSOR_COUNT = 100_000L;
	private double probObs;

	public ClientCommunicationSource(final int maxRecordsPerSecond, double obsProb) {
		super(maxRecordsPerSecond);
		this.probObs = obsProb;
	}

	private static List<String> fName =  Arrays.asList("Romeo", "Blake", "Aarav", "Cillian", "Lorenzo", "Aleksander", "Vincent", "Ted", "Elliot",
			"Ralphy", "Melissa", "Dolly", "Fern", "Hollie", "Evie", "Nancy", "Daniella", "Olivia-Rose", "Delilah", "Norah", "Jo", "Skylar", "Ali", "Kit", "Jackie", "Glenn", "Kai", "Alex", "Charlie", "Reggie");
	private static List<String> lName = Arrays.asList("Harvey", "Baker", "Saunders", "Green", "Houghton", "Kennedy", "Russell", "Wood", "Cole", "Kennedy", "Berry",
			"Day", "Mitchell", "Jackson", "Chambers", "Robertson", "Matthews", "Bailey", "Reynolds", "Lloyd", "Johnston", "Lloyd", "Bates", "Ward", "Hunter", "Harper", "Austin", "Kennedyv", "Fletcher", "Bates");

	@Override
	protected CustomerReport randomEvent(SplittableRandom rnd, long id) {
		double random = rnd.nextDouble();
		String nexState = null;
		if(random < probObs){
			nexState = "BROKEN_DOWN";
		}else if((random > probObs) && random < (probObs + ((1-probObs)/4))){
			nexState = "DISSATISFIED";
		}else{
			nexState = "SATISFIED";
		}

		long vehicleId = rnd.nextLong(SENSOR_COUNT)/100;
		return CustomerReport.builder().vehicleId(vehicleId)
				.timestamp(System.currentTimeMillis() - rnd.nextLong(1000L))
				.value(nexState)
				.customerName(fName.get((int) vehicleId % fName.size()) + " " + lName.get((int) vehicleId % lName.size()))
				.build();
	}
}
