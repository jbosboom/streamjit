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
				printStats(inputNotFirable);
				System.out.println(line);
			}
			if (line.contains("Not firable")) {
				String t = token(line);
				if (!inputNotFirable.containsKey(t))
					inputNotFirable.put(t, 0);
				int val = inputNotFirable.get(t);
				inputNotFirable.put(t, ++val);
				writer.write(line);
				writer.write("\n");
			}
		}
		printStats(inputNotFirable);
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static void printStats(Map<String, Integer> countMap) {
		for (Map.Entry<String, Integer> en : countMap.entrySet()) {
			System.out.println(String.format("%s-%d", en.getKey(),
					en.getValue()));
		}
		countMap.clear();
	}

	private static String token(String line) {
		int start = line.indexOf('(');
		int end = line.indexOf(')');
		return String.format("Token%s", line.substring(start, end + 1));
	}
}
