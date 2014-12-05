package edu.mit.streamjit.util;

import java.io.BufferedReader;
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
		List<Integer> compileTime = processCompileTime();
		List<Integer> runTime = processRunTime();
		List<Integer> drainTime = processDrainTime();
		List<Integer> sn1HeapMax = processSNHeap("st1.txt", true);
		List<Integer> sn2HeapMax = processSNHeap("st2.txt", true);
		List<Integer> sn1HeapSize = processSNHeap("st1.txt", false);
		List<Integer> sn2HeapSize = processSNHeap("st2.txt", false);

		FileWriter writer = new FileWriter("totalStats.txt");
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < runTime.size(); i++) {
			min = Math.min(min, runTime.get(i));
			String msg = String.format(
					"%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t\n",
					i + 1, compileTime.get(i), runTime.get(i),
					drainTime.get(i), sn1HeapMax.get(i), sn1HeapSize.get(i),
					sn2HeapMax.get(i), sn2HeapSize.get(i), min);

			writer.write(msg);
		}
		writer.close();

		writeHeapStat("st1.txt");
		writeHeapStat("st2.txt");
	}

	private static List<Integer> processCompileTime() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				"FMRadioCore_compileTime.txt"));
		FileWriter writer = new FileWriter("CompileTime.txt");
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

	private static List<Integer> processRunTime() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				"FMRadioCore_runTime.txt"));
		FileWriter writer = new FileWriter("RunTime.txt");
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

	private static List<Integer> processDrainTime() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				"FMRadioCore_drainTime.txt"));
		FileWriter writer = new FileWriter("DrainTime.txt");
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
}
