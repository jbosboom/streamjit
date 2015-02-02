package edu.mit.streamjit.impl.distributed.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.IOInfo;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class Utils {

	public static Token getBlobID(Blob b) {
		return Collections.min(b.getInputs());
	}

	public static Token getblobID(Set<Worker<?, ?>> workers) {
		ImmutableSet.Builder<Token> inputBuilder = new ImmutableSet.Builder<>();
		for (IOInfo info : IOInfo.externalEdges(workers)) {
			if (info.isInput())
				inputBuilder.add(info.token());
		}

		return Collections.min(inputBuilder.build());
	}

	/**
	 * Prints heapMaxSize, current heapSize and heapFreeSize.
	 */
	public static void printMemoryStatus() {
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		long heapSize = Runtime.getRuntime().totalMemory();
		long heapFreeSize = Runtime.getRuntime().freeMemory();
		int MEGABYTE = 1024 * 1024;
		System.out.println("#########################");
		printCurrentDateTime();
		System.out.println(String.format("heapMaxSize = %dMB", heapMaxSize
				/ MEGABYTE));
		System.out.println(String
				.format("heapSize = %dMB", heapSize / MEGABYTE));
		System.out.println(String.format("heapFreeSize = %dMB", heapFreeSize
				/ MEGABYTE));
		System.out.println("#########################");
	}

	/**
	 * Prints current date and time in "yyyy/MM/dd HH:mm:ss" format.
	 */
	public static void printCurrentDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));
	}

	public static void printOutOfMemory() {
		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		System.out.println("******OutOfMemoryError******");
		MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
		int MEGABYTE = 1024 * 1024;
		long maxMemory = heapUsage.getMax() / MEGABYTE;
		long usedMemory = heapUsage.getUsed() / MEGABYTE;
		System.out
				.println("Memory Use :" + usedMemory + "M/" + maxMemory + "M");
	}

	/**
	 * @param name
	 *            name of the directory.
	 * @return <code>true</code> if and only if the directory was created; false
	 *         otherwise.
	 */
	private static boolean createDir(String name) {
		File dir = new File(name);
		if (dir.exists()) {
			if (dir.isDirectory())
				return true;
			else {
				System.err.println("A file exists in the name of dir-" + name);
				return false;
			}
		} else
			return dir.mkdirs();
	}

	/**
	 * Creates app directory with the name of appName, and creates a sub
	 * directory "configurations".
	 * 
	 * @param name
	 *            name of the directory.
	 * @return <code>true</code> if and only if the directories were created;
	 *         false otherwise.
	 */
	public static boolean createAppDir(String appName) {
		if (createDir(appName))
			return createDir(String.format("%s%sconfigurations", appName,
					File.separator));
		else
			return false;
	}

	/**
	 * Writes README.txt. Mainly saves GlobalConstant values.
	 * 
	 * @param appName
	 */
	public static void writeReadMeTxt(String appName) {
		try {
			FileWriter writer = new FileWriter(String.format("%s%sREADME.txt",
					appName, File.separator));
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			writer.write(dateFormat.format(cal.getTime()) + "\n");
			writer.write(appName + "\n");
			update(writer, "tunerStartMode", GlobalConstants.tunerStartMode);
			update(writer, "useDrainData", GlobalConstants.useDrainData);
			update(writer, "needDrainDeadlockHandler",
					GlobalConstants.needDrainDeadlockHandler);
			update(writer, "tune", GlobalConstants.tune);
			update(writer, "saveAllConfigurations",
					GlobalConstants.saveAllConfigurations);
			update(writer, "outputCount", GlobalConstants.outputCount);
			update(writer, "useCompilerBlob", GlobalConstants.useCompilerBlob);
			update(writer, "printOutputCountPeriod",
					GlobalConstants.printOutputCountPeriod);
			update(writer, "singleNodeOnline", GlobalConstants.singleNodeOnline);
			update(writer, "maxNumCores", GlobalConstants.maxNumCores);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void update(FileWriter writer, String name, int val)
			throws IOException {
		writer.write(String.format("%s=%d\n", name, val));
	}

	private static void update(FileWriter writer, String name, boolean val)
			throws IOException {
		writer.write(String.format("%s=%s\n", name, val ? "True" : "False"));
	}

	/**
	 * Creates and returns a {@link FileWriter} with append = false. Suppresses
	 * {@link IOException} and returns null if exception occurred. This method
	 * is added to keep other classes clean.
	 * 
	 * @param name
	 * @return
	 */
	public static FileWriter fileWriter(String name) {
		return fileWriter(name, false);
	}

	/**
	 * Creates and returns a {@link FileWriter}. Suppresses {@link IOException}
	 * and returns null if exception occurred. This method is added to keep
	 * other classes clean.
	 * 
	 * @param name
	 * @return
	 */
	public static FileWriter fileWriter(String name, boolean append) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(name, append);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fw;
	}
}
