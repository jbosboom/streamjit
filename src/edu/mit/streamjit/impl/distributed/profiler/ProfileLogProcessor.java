package edu.mit.streamjit.impl.distributed.profiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Just an utility that processes profile.txt.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
public class ProfileLogProcessor {

	public static void main(String[] args) throws IOException {
		String appName = "FilterBankPipeline";
		// process1(appName);
		process2(appName);
	}

	private static List<Integer> process1(String appName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%sprofile.txt", appName, File.separator)));
		FileWriter writer = new FileWriter(String.format(
				"%s%sProcessedProfile.txt", appName, File.separator));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("--------------------------------")) {
				writer.write(line);
				writer.write("\n");
			}
			if (line.contains("Not firable")) {
				writer.write(line);
				writer.write("\n");
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static List<Integer> process2(String appName) throws IOException {
		Map<String, Integer> inputNotFirable = new HashMap<>();
		Map<String, Integer> outputNotFirable = new HashMap<>();
		Map<String, Integer> notFirable = inputNotFirable;
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%sprofile.txt", appName, File.separator)));
		FileWriter writer = new FileWriter(String.format(
				"%s%sProcessedProfile.txt", appName, File.separator));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("--------------------------------")) {
				writer.write(line);
				writer.write("\n");
				printStats(inputNotFirable, outputNotFirable);
				System.out.println(line);
			} else if (line.contains("Input..."))
				notFirable = inputNotFirable;
			else if (line.contains("Output..."))
				notFirable = outputNotFirable;
			if (line.contains("Not firable")) {
				String t = token(line);
				add(notFirable, t);
				writer.write(line);
				writer.write("\n");
			}
		}
		printStats(inputNotFirable, outputNotFirable);
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static void add(Map<String, Integer> notFirable, String t) {
		if (!notFirable.containsKey(t))
			notFirable.put(t, 0);
		int val = notFirable.get(t);
		notFirable.put(t, ++val);
	}

	private static void printStats(Map<String, Integer> inputNotFirable,
			Map<String, Integer> outputNotFirable) {
		System.out.println("Input...");
		printStats(inputNotFirable);
		System.out.println("Output...");
		printStats(outputNotFirable);
	}

	private static void printStats(Map<String, Integer> notFirable) {
		for (Map.Entry<String, Integer> en : notFirable.entrySet()) {
			System.out.println(String.format("\t%s-%d", en.getKey(),
					en.getValue()));
		}
		notFirable.clear();
	}

	private static String token(String line) {
		int start = line.indexOf('(');
		int end = line.indexOf(')');
		return String.format("Token%s", line.substring(start, end + 1));
	}
}
