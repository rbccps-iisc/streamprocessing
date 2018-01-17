package robertbosch.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.protobuf.util.JsonFormat;
import com.protoTest.smartcity.Actuated;

public class Testproto {
	
	public static void main(String[] args) {
		//writeProtodata();
		readProtoData();
	}
	
	private static void readProtoData() {
		
		try {
			Actuated.targetConfigurations confs = Actuated.targetConfigurations.parseFrom(new FileInputStream("/Users/sahiltyagi/Desktop/out.txt"));
			System.out.println(confs.getPowerState().getTargetPowerState());
			System.out.println(confs.getControlPolicy().getControlPolicy());
			System.out.println(confs.getManualControlParams().getTargetBrightnessLevel());
			
			
			Object ob = Actuated.targetConfigurations.parseFrom(new FileInputStream("/Users/sahiltyagi/Desktop/out.txt"));
			String packet=JsonFormat.printer().print((Actuated.targetConfigurations)ob);
			System.out.println(packet);
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void readInRemoteMode() {
		
		
	}
	
	private static void writeProtodata() {
		
		Actuated.targetPowerStateParams.Builder powerstate = Actuated.targetPowerStateParams.newBuilder();
		powerstate.setTargetPowerState(true);
		
		Actuated.targetControlPolicy.Builder ctrlpolicy = Actuated.targetControlPolicy.newBuilder();
		ctrlpolicy.setControlPolicy(Actuated.ctrlPolicy.AUTO_LUX);
		
		Actuated.targetManualControlParams.Builder manualparams = Actuated.targetManualControlParams.newBuilder();
		manualparams.setTargetBrightnessLevel(99);
		
		Actuated.targetAutoTimerParams.Builder autotimers = Actuated.targetAutoTimerParams.newBuilder();
		autotimers.setTargetOnTime(100);
		autotimers.setTargetOffTime(150);
		
		Actuated.targetAutoLuxParams.Builder autolux = Actuated.targetAutoLuxParams.newBuilder();
		autolux.setTargetOnLux(199);
		autolux.setTargetOffLux(299);
		
		Actuated.targetConfigurations.Builder confs = Actuated.targetConfigurations.newBuilder();
		confs.setPowerState(powerstate);
		confs.setControlPolicy(ctrlpolicy);
		confs.setManualControlParams(manualparams);
		confs.setAutoTimerParams(autotimers);
		confs.setAutoLuxParams(autolux);
		
		Actuated.targetConfigurations finalconf = confs.build();
		try {
			
			finalconf.writeTo(new FileOutputStream("/Users/sahiltyagi/Desktop/out.txt"));
			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("done writing proto data to file");
	}
	
}