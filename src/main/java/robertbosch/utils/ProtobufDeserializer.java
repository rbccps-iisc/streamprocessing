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

public class ProtobufDeserializer {
	
	public static String deserialize(byte[] buffer, String mainclass, String message) {
		String deserData=null;
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
			Object data = convertmethod.invoke(printer, packet);
			deserData = data.toString();
			
		} catch(ClassNotFoundException c) {
			c.printStackTrace();
		} catch(NoSuchMethodException method) {
			method.printStackTrace();
		} catch(IllegalAccessException acc) {
			acc.printStackTrace();
		} catch(InvocationTargetException invoke) {
			invoke.printStackTrace();
		}
		
		return deserData.toString();
	}
	
	private static void generateProtobufClasses(String url) {
		try {
			URL link = new URL(url);
			BufferedReader rdr = new BufferedReader(new InputStreamReader(link.openStream()));
			String proto;
			String protofile = RobertBoschUtils.protofiles + url.split("/")[url.split("/").length -1];
			BufferedWriter bfrwrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(protofile)));
			
			while((proto = rdr.readLine()) != null) {
				bfrwrtr.write(proto+"\n");
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
			
		} catch(MalformedURLException urlex) {
			urlex.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(InterruptedException in) {
			in.printStackTrace();
		}
	}
	
	public static void performActionOnData() {
		//if list in supervisor task does not contain proto file name (upper case), then run  generateProtobufClasses(url method), followed by deserializer method
		//otherwise, run deserializer method ONLY
		
		
	}
	
}
