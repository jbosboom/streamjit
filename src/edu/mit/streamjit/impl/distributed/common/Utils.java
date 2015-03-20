/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.common;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.HotSpotTuning;
import edu.mit.streamjit.impl.distributed.PartitionManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.test.apps.fmradio.FMRadio;
import edu.mit.streamjit.util.ConfigurationUtils;

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
	public static boolean createDir(String name) {
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
			return createDir(String.format("%s%s%s", appName, File.separator,
					ConfigurationUtils.configDir));
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
			// rename(appName, "README.txt");
			FileWriter writer = new FileWriter(String.format("%s%sREADME.txt",
					appName, File.separator));
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			writer.write(dateFormat.format(cal.getTime()) + "\n");
			writer.write(appName + "\n");
			Properties prop = Options.getProperties();
			prop.store(writer, "GlobalConstants.Properties");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return true iff renaming is success.
	 */
	public static boolean rename(String appName, String fileName) {
		File file = new File(String.format("%s%s%s", appName, File.separator,
				fileName));
		File fileOrig = new File(String.format("%s%s%s.orig", appName,
				File.separator, fileName));
		if (fileOrig.exists())
			return false;
		if (file.exists())
			file.renameTo(fileOrig);
		return true;
	}

	/**
	 * Returns a {@link FileWriter} of the file "dirName/fileName" with append =
	 * false. Creates the file if it not exists. Suppresses {@link IOException}
	 * and returns null if exception occurred. This method is added to keep
	 * other classes clean.
	 * 
	 * @return {@link FileWriter} or null.
	 */
	public static FileWriter fileWriter(String dirName, String fileName) {
		return fileWriter(dirName, fileName, false);
	}

	/**
	 * Returns a {@link FileWriter} of the file "dirName/fileName". Creates the
	 * file if it not exists. Suppresses {@link IOException} and returns null if
	 * exception occurred. This method is added to keep other classes clean.
	 * 
	 * @return {@link FileWriter} or null.
	 */
	public static FileWriter fileWriter(String dirName, String fileName,
			boolean append) {
		String fullFileName = String.format("%s%s%s", dirName, File.separator,
				fileName);
		return fileWriter(fullFileName, append);
	}
	/**
	 * Creates and returns a {@link FileWriter} with append = false. Suppresses
	 * {@link IOException} and returns null if exception occurred. This method
	 * is added to keep other classes clean.
	 * 
	 * @return {@link FileWriter} or null.
	 */
	public static FileWriter fileWriter(String name) {
		return fileWriter(name, false);
	}

	/**
	 * Creates and returns a {@link FileWriter}. Suppresses {@link IOException}
	 * and returns null if exception occurred. This method is added to keep
	 * other classes clean.
	 * 
	 * @return {@link FileWriter} or null.
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

	/**
	 * [16-02-2015] - I couldn't run dot tools in Lanka cluster. So as a hack, i
	 * implemented this method to generate blob graph for each configuration.
	 * TODO: This generation process is damn slow. Takes 40 mins to process 5000
	 * cfgs.
	 * 
	 * @param stream
	 * @throws IOException
	 */
	public static void generateBlobGraphs(OneToOneElement<?, ?> stream)
			throws IOException {
		StreamJitApp<?, ?> app = new StreamJitApp<>(stream);
		PartitionManager partitionManager = new HotSpotTuning(app);
		partitionManager.getDefaultConfiguration(
				Workers.getAllWorkersInGraph(app.source), 2);
		ConfigurationManager cfgManager = new ConfigurationManager(app,
				partitionManager);
		Stopwatch sw = Stopwatch.createStarted();
		for (Integer i = 1; i < 5010; i++) {
			String prefix = i.toString();
			Configuration cfg = ConfigurationUtils.readConfiguration(app.name,
					prefix);
			if (cfg != null) {
				cfg = ConfigurationUtils.addConfigPrefix(cfg, prefix);
				cfgManager.newConfiguration(cfg);
			}
		}

		Configuration cfg = ConfigurationUtils.readConfiguration(app.name,
				"final");
		if (cfg != null) {
			cfg = ConfigurationUtils.addConfigPrefix(cfg, "final");
			cfgManager.newConfiguration(cfg);
		}
		sw.stop();
		System.out.println(sw.elapsed(TimeUnit.SECONDS));
	}

	public static void main(String[] args) throws IOException {
		generateBlobGraphs(new FMRadio.FMRadioCore());
	}

	/**
	 * Backups the files generated during tuning.
	 */
	public static void backup(String appName) {
		rename(appName, "summary");
		rename(appName, "compileTime.txt");
		rename(appName, "runTime.txt");
		rename(appName, "drainTime.txt");
		rename(appName, "GraphProperty.txt");
		rename(appName, "profile.txt");
	}

	/**
	 * Move all files and directories, except the configuration directory, from
	 * appDir to appDir/tune directory. Does nothing if tune directory exists.
	 * 
	 * @param appName
	 */
	public static void backup1(String appName) {
		File[] listOfFilesMove = listOfFilesMove(appName);
		if (listOfFilesMove.length == 0)
			return;

		File tuneDir = new File(String.format("%s%stune", appName,
				File.separator));
		if (tuneDir.exists())
			return;

		if (!createDir(tuneDir.getPath()))
			System.err.println(String.format("Creating %s dir failed.",
					tuneDir.getPath()));
		for (File f : listOfFilesMove) {
			try {
				Files.move(f.toPath(),
						Paths.get(tuneDir.getPath(), f.getName()),
						REPLACE_EXISTING);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static File[] listOfFilesMove(final String appName) {
		File dir = new File(appName);
		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return !name.equals(ConfigurationUtils.configDir);
			}
		});
		return files;
	}

	public static void newApp(String appName) {
		createAppDir(appName);
		backup1(appName);
		Utils.writeReadMeTxt(appName);
	}
}
