package edu.mit.streamjit.impl.common;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;

import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.util.ConfigurationUtils;

public class TuningStatistics {

	private static String[] boolParams = { "remove", "fuse", "unboxStorage",
			"unboxInput", "unboxOutput" };

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// printCfgValues("1NestedSplitJoinCore.cfg");
		printAll("../Tuner layer/tuning-oopsla2014");

	}

	private static Double getBoolParamStat(Map<String, Parameter> parameters,
			String prefix) {
		int totalParams = 0;
		int noOfTRUE = 0;

		for (Map.Entry<String, Parameter> en : parameters.entrySet()) {
			if (en.getKey().startsWith(prefix)) {
				totalParams++;
				SwitchParameter<Boolean> p = (SwitchParameter<Boolean>) en
						.getValue();
				if (p.getValue()) {
					noOfTRUE++;
				}
			}
		}
		double per = 100 * (double) noOfTRUE / totalParams;
		System.out.println("totalParams - " + totalParams);
		System.out.println("noOfTRUE - " + noOfTRUE);
		System.out.println("Percentage - " + per);
		return per;
	}

	private static void printAll(String folderPath) throws IOException {
		File folder = new File(folderPath);
		File[] listOfFiles = folder.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.toLowerCase().endsWith(".cfg");
			}
		});
		System.out.println(String.format("Parameters in configuration..."));

		FileWriter writer = new FileWriter("paramStat.dat", false);
		writeHeader(writer);
		for (int i = 0; i < listOfFiles.length; i++) {
			printCfgValues(listOfFiles[i].getAbsolutePath(), writer);
		}
	}

	private static void printCfgValues(String fileName, FileWriter writer)
			throws IOException {
		Configuration cfg = ConfigurationUtils.readConfiguration(fileName);
		if (cfg != null) {
			File f = new File(fileName);
			Map<String, Parameter> parameters = cfg.getParametersMap();
			writer.write("\n");
			writer.write(f.getName());
			System.out.println(String.format("%s - %d", f.getName(), parameters
					.entrySet().size()));

			for (int i = 0; i < boolParams.length; i++) {
				writer.write("\t");
				writer.write(getBoolParamStat(parameters, boolParams[i])
						.toString());
			}
			writer.flush();
		}
	}

	private static void writeHeader(FileWriter writer) throws IOException {
		writer.write("\t\t");
		for (int i = 0; i < boolParams.length; i++) {
			writer.write("\t");
			writer.write(boolParams[i]);
		}
		writer.flush();
	}
}
