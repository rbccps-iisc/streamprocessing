package robertbosch.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import com.google.protobuf.util.JsonFormat;
import com.protoTest.smartcity.Actuated;

public class Testproto {
	
	public static void main(String[] args) {
		//writeProtodata();
		//readProtoData();
		readInRemoteMode();
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
		try {
			
			Class cls = Class.forName("com.protoTest.smartcity.Actuated$targetConfigurations");
//			for(Method method : cls.getMethods()) {
//				System.out.println(method.getName());
//			}
			
			Class[] arr = {InputStream.class};
			Method method = cls.getDeclaredMethod("parseFrom", arr);
			Object packet = method.invoke(cls, new FileInputStream("/Users/sahiltyagi/Desktop/out.txt"));
			//System.out.println(packet.toString());
			
			Class format = Class.forName("com.google.protobuf.util.JsonFormat");
			Class[] arr2 = {};
			Method m2 = format.getDeclaredMethod("printer", arr2);
			Object printer = m2.invoke(format, null);
//			System.out.println(printer.getClass());			
//			com.google.protobuf.MessageOrBuilder
//			for(Method m : printer.getClass().getMethods()) {
//				System.out.println(m.getName());
//				for(Parameter param : m.getParameters()) {
//					System.out.println(param.getParameterizedType().getTypeName());
//				}
//			}
			
			Class[] arr3 = {Class.forName("com.google.protobuf.MessageOrBuilder")};
			Method m3 = printer.getClass().getDeclaredMethod("print", arr3);
			Object data = m3.invoke(printer, packet);
			System.out.println(data.toString());
			System.out.println("done.");
			
		} catch(ClassNotFoundException c) {
			c.printStackTrace();
		} catch(NoSuchMethodException method) {
			method.printStackTrace();
		} catch(FileNotFoundException f) {
			f.printStackTrace();
		} catch(IllegalAccessException acc) {
			acc.printStackTrace();
		} catch(InvocationTargetException invoke) {
			invoke.printStackTrace();
		}
	}
	
	private static void dynamicCompile() {
		
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