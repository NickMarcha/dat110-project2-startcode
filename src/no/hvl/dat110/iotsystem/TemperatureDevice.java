package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.common.TODO;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor TS = new TemperatureSensor();

		// TODO - start

		// create a client object and use it to

		Client client = new Client("temperatursensor", Common.BROKERHOST,Common.BROKERPORT);
		client.connect();
		
		for	(int i = 0; i < 10; i ++) {
			client.publish("temperature",  Integer.toString(TS.read()));
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// - connect to the broker
		// - publish the temperature(s)
		
		// - disconnect from the broker
		client.disconnect();
		// TODO - end

		System.out.println("Temperature device stopping ... ");


	}
}
