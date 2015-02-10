package edu.mit.streamjit.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Processes the Distributed StreamJit's time log files and generate summary.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Dec 5, 2014
 */
public class TimeLogProcessor {

	public static void main(String[] args) throws IOException {
		summarize("FMRadioCore");
	}

	private static List<Integer> processCompileTime(String appName)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%scompileTime.txt", appName, File.separator)));
		FileWriter writer = new FileWriter(String.format(
				"%s%sProcessedCompileTime.txt", appName, File.separator));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("Total")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.add(val);
				String data = String.format("%d\t%d\n", ++i, val);
				writer.write(data);
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static List<Integer> processRunTime(String appName)
			throws IOException {

		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%srunTime.txt", appName, File.separator)));
		FileWriter writer = new FileWriter(String.format(
				"%s%sProcessedRunTime.txt", appName, File.separator));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		int min = Integer.MAX_VALUE;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("Execution")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				if (val < 1)
					val = 2 * min;
				min = Math.min(min, val);
				ret.add(val);
				String data = String
						.format("%-6d\t%-6d\t%-6d\n", ++i, val, min);
				writer.write(data);
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static List<Integer> processDrainTime(String appName)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%sdrainTime.txt", appName, File.separator)));
		FileWriter writer = new FileWriter(String.format(
				"%s%sProcessedDrainTime.txt", appName, File.separator));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("Drain time")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.add(val);
				String data = String.format("%d\t%d\n", ++i, val);
				writer.write(data);
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static void writeHeapStat(String fileName) throws IOException {
		List<Integer> heapSize = processSNHeap(fileName, false);
		List<Integer> heapMaxSize = processSNHeap(fileName, true);
		FileWriter writer = new FileWriter(String.format("%s_heapStatus.txt",
				fileName));
		for (int i = 0; i < heapSize.size(); i++) {
			String msg = String.format("%-6d\t%-6d\t%-6d\n", i + 1,
					heapSize.get(i), heapMaxSize.get(i));
			writer.write(msg);
		}
		writer.close();
	}

	private static List<Integer> processSNHeap(String fileName,
			Boolean isHeapMax) throws IOException {
		String heapType = "heapSize";
		if (isHeapMax)
			heapType = "heapMaxSize";
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith(heapType)) {
				String[] arr = line.split(" ");
				String time = arr[2].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.add(val);
			}
		}
		reader.close();
		return ret;
	}

	private static void summarize(String appName) throws IOException {
		List<Integer> compileTime = processCompileTime(appName);
		List<Integer> runTime = processRunTime(appName);
		List<Integer> drainTime = processDrainTime(appName);

		FileWriter writer = new FileWriter(String.format("%s%stotalStats.txt",
				appName, File.separator));
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < runTime.size(); i++) {
			min = Math.min(min, runTime.get(i));
			String msg = String.format("%-6d\t%-6d\t%-6d\t%-6d\t%-6d\n", i + 1,
					compileTime.get(i), runTime.get(i), drainTime.get(i), min);

			writer.write(msg);
		}
		writer.close();

		// writeHeapStat(String.format("%s%sst1.txt", appName, File.separator));
		// writeHeapStat(String.format("%s%sst2.txt", appName, File.separator));
	}
}
