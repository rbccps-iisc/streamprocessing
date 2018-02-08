package robertbosch.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;

import robertbosch.schema.validation.NetworkserverSpout;
import robertbosch.schema.validation.SchemaVerifyBolt;

public class ProtobufDeserializer {
	
//	public static void main(String[] args) {
//		//String url = "https://raw.githubusercontent.com/mukuntharun/flowsensor/master/protos/sensed.proto";
//		String url = "https://raw.githubusercontent.com/rbccps-iisc/applications-streetlight/master/proto_stm/txmsg/sensed.proto";
//		generateProtobufClasses(url);
//	}
	
	private static void generateProtobufClasses(String url) {
		try {
			System.out.println("reading url...");
			URL link = new URL(url);
			BufferedReader rdr = new BufferedReader(new InputStreamReader(link.openStream()));
			String proto, entirestring ="";
			String protofile = RobertBoschUtils.protofiles + url.split("/")[url.split("/").length -1];
			BufferedWriter bfrwrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(protofile)));
			boolean proto2syntax =true;
			
			while((proto = rdr.readLine()) != null) {
				
				entirestring += proto + "\n";
				
				if(entirestring.startsWith("syntax=\"proto2\";\n")) {
					bfrwrtr.write(proto+"\n");
					if(proto2syntax) {
						bfrwrtr.write("option java_package= \"com.protoTest.smartcity\";\n");
						proto2syntax=false;
					}
				} else {
					if(proto2syntax) {
						bfrwrtr.write("option java_package= \"com.protoTest.smartcity\";\n");
						proto2syntax=false;
					}
					
					bfrwrtr.write(proto+"\n");
				}
				
			}
			rdr.close();
			bfrwrtr.close();
			
			String[] command = {RobertBoschUtils.props.getProperty("protocompiler"), "--proto_path=" + RobertBoschUtils.props.getProperty("protopath"), 
								"--java_out=" + RobertBoschUtils.props.getProperty("javapath"), protofile};
			
			Process proc = Runtime.getRuntime().exec(command);
			int waitime = proc.waitFor();
			
			ProcessBuilder builder = new ProcessBuilder(RobertBoschUtils.props.getProperty("maven"), "clean", "compile", "assembly:single");
			builder.directory(new File(RobertBoschUtils.props.getProperty("schemarepo")));
			proc = builder.start();
			waitime = proc.waitFor();
			System.out.println("...................................................... generated protobuf classes");
			
		} catch(MalformedURLException urlex) {
			urlex.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(InterruptedException in) {
			in.printStackTrace();
		}
	}
	
	public static String deserialize(byte[] buffer, String message, String url) {
		//if list in supervisor task does not contain proto file name (uppercase), then run  generateProtobufClasses(url method), followed by deserializer method
		//otherwise, run deserializer method ONLY
		if(!NetworkserverSpout.protoURLs.contains(url)) {
			System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ going to generate protobuf classes");
			generateProtobufClasses(url);
			NetworkserverSpout.protoURLs.add(url);
		} else {
			System.out.println("NOT GOING TO GENERATE PROTO CLASSES@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		}
		
		String protofile= url.split("/")[url.split("/").length -1].split(".proto")[0];
		String mainclass= protofile.substring(0, 1).toUpperCase() + protofile.substring(1);
		
		if(mainclass.equalsIgnoreCase(message)) {
			mainclass = mainclass + "OuterClass";
		}
		
		Object data=null;
		try {
			Class cls = Class.forName("com.protoTest.smartcity."+mainclass+"$"+message);
			Class[] arr = {buffer.getClass()};
			Method parsemethod = cls.getDeclaredMethod("parseFrom", arr);
			Object packet = parsemethod.invoke(cls, buffer);
			
			Class format = Class.forName("com.google.protobuf.util.JsonFormat");
			Class[] arr2 = {};
			Method printmethod = format.getDeclaredMethod("printer", arr2);
			Object printer = printmethod.invoke(format, null);
			
			Class[] arr3 = {Class.forName("com.google.protobuf.MessageOrBuilder")};
			Method convertmethod = printer.getClass().getDeclaredMethod("print", arr3);
			data = convertmethod.invoke(printer, packet);
			
		} catch(ClassNotFoundException c) {
			c.printStackTrace();
		} catch(NoSuchMethodException method) {
			method.printStackTrace();
		} catch(IllegalAccessException acc) {
			acc.printStackTrace();
		} catch(InvocationTargetException invoke) {
			invoke.printStackTrace();
		}
		
		return data.toString();
	}
	
}
