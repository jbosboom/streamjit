package edu.mit.streamjit.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Processes the Distributed StreamJit's time log files and generate summary.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Dec 5, 2014
 */
public class TimeLogProcessor {

	private static final String pRunTimeFile = "processedRunTime.txt";
	private static final String pCompTimeFile = "processedCompileTime.txt";
	private static final String pDrainTimeFile = "processedDrainTime.txt";
	private static final String pTuneRoundTimeFile = "processedTuningRoundTime.txt";
	private static final String pEvalTimeFile = "processedEvaluation.txt";
	private static final String ptotalFile = "totalStats.txt";

	public static void main(String[] args) throws IOException {
		summarize("FMRadioCore");
		summarizeHeap("FMRadioCore");
	}

	private static Map<String, Integer> processCompileTime(String appName,
			File outDir) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%scompileTime.txt", appName, File.separator)));

		File outFile = new File(outDir, pCompTimeFile);
		FileWriter writer = new FileWriter(outFile, false);
		Map<String, Integer> ret = process(reader, writer, "Total", true, 3);
		reader.close();
		writer.close();
		return ret;
	}

	private static String cfgString(String line) {
		String l = line.replace('-', ' ');
		return l.trim();
	}

	private static Map<String, Integer> processRunTime(String appName,
			File outDir) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%srunTime.txt", appName, File.separator)));
		File outFile = new File(outDir, pRunTimeFile);
		FileWriter writer = new FileWriter(outFile, false);
		String line;
		String cfgPrefix = "Init";
		int i = 0;
		Map<String, Integer> ret = new HashMap<>(5000);
		int min = Integer.MAX_VALUE;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("----------------------------"))
				cfgPrefix = cfgString(line);
			else if (line.startsWith("Execution")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				if (val < 1)
					val = 2 * min;
				min = Math.min(min, val);
				ret.put(cfgPrefix, val);
				String data = String.format("%-6d\t%-6s\t%-6d\t%-6d\n", ++i,
						cfgPrefix, val, min);
				writer.write(data);
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}
	private static Map<String, Integer> processDrainTime(String appName,
			File outDir) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%sdrainTime.txt", appName, File.separator)));
		File outFile = new File(outDir, pDrainTimeFile);
		FileWriter writer = new FileWriter(outFile, false);
		Map<String, Integer> ret = process(reader, writer, "Drain time", true,
				3);
		writer.flush();
		reader.close();
		writer.close();
		return ret;
	}

	private static Map<String, Integer> processTuningRoundTime(String appName,
			File outDir) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%srunTime.txt", appName, File.separator)));
		File outFile = new File(outDir, pTuneRoundTimeFile);
		FileWriter writer = new FileWriter(outFile, false);
		Map<String, Integer> ret = process(reader, writer, "Tuning", false, 3);
		reader.close();
		writer.close();
		return ret;
	}

	private static Map<String, Integer> process(BufferedReader reader,
			FileWriter writer, String startString, boolean isms, int timepos)
			throws IOException {
		String line;
		String cfgPrefix = "Init";
		int i = 0;
		int timeUnitLength = 1;
		if (isms)
			timeUnitLength = 2;
		Map<String, Integer> ret = new HashMap<>(5000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("----------------------------"))
				cfgPrefix = cfgString(line);
			else if (line.startsWith(startString)) {
				String[] arr = line.split(" ");
				String time = arr[timepos].trim();
				time = time.substring(0, time.length() - timeUnitLength);
				int val = Integer.parseInt(time);
				ret.put(cfgPrefix, val);
				String data = String
						.format("%d\t%s\t%d\n", ++i, cfgPrefix, val);
				writer.write(data);
			}
		}
		writer.flush();
		return ret;
	}

	private static File writeHeapStat(String fileName, File outDir)
			throws IOException {
		List<Integer> heapSize = processSNHeap(fileName, "heapSize");
		List<Integer> heapMaxSize = processSNHeap(fileName, "heapMaxSize");
		List<Integer> heapFreeSize = processSNHeap(fileName, "heapFreeSize");
		File f = new File(fileName);
		String outFileName = String.format("%s_heapStatus.txt", f.getName());
		File outFile = new File(outDir, outFileName);
		FileWriter writer = new FileWriter(outFile, false);
		for (int i = 0; i < heapSize.size(); i++) {
			String msg = String.format("%-6d\t%-6d\t%-6d\t%-6d\n", i + 1,
					heapFreeSize.get(i), heapSize.get(i), heapMaxSize.get(i));
			writer.write(msg);
		}
		writer.close();
		return outFile;
	}

	private static List<Integer> processSNHeap(String fileName, String heapType)
			throws IOException {
		String slurmPrefix = "0: ";
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			// Slurm adds prefix to every sysout line.
			if (line.startsWith(slurmPrefix))
				line = line.substring(slurmPrefix.length());
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

	public static void summarize(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		Map<String, Integer> compileTime = processCompileTime(appName,
				summaryDir);
		Map<String, Integer> runTime = processRunTime(appName, summaryDir);
		Map<String, Integer> drainTime = processDrainTime(appName, summaryDir);
		Map<String, Integer> tuningRoundTime = processTuningRoundTime(appName,
				summaryDir);

		File outfile = new File(summaryDir, ptotalFile);
		FileWriter writer = new FileWriter(outfile, false);
		FileWriter verify = new FileWriter(String.format("%s%sverify.txt",
				appName, File.separator));
		int min = Integer.MAX_VALUE;
		writer.write("cfg\tTRTime\tcomp\trun\tdrain\tmin\n");
		for (int i = 1; i <= tuningRoundTime.size(); i++) {
			String key = new Integer(i).toString();
			Integer time = runTime.get(key);

			if (time == null) {
				System.err.println("No running time for round " + key);
				continue;
			} else if (time < min) {
				verify.write(String.format("%s=%d\n", key, time));
				min = time;
			}

			String msg = String.format("%-6d\t%-6d\t%-6d\t%-6d\t%-6d\t%-6d\n",
					i, tuningRoundTime.get(key), compileTime.get(key),
					runTime.get(key), drainTime.get(key), min);
			writer.write(msg);
		}
		verify.close();
		writer.close();

		File f = createTotalStatsPlotFile(summaryDir, appName);
		plot(summaryDir, f);
		f = createProcessedPlotFile(summaryDir, appName);
		plot(summaryDir, f);
	}

	/**
	 * Creates plot file for {@link #ptotalFile}.
	 */
	private static File createTotalStatsPlotFile(File dir, String appName)
			throws IOException {
		String title = getTitle(appName);
		boolean pdf = true;
		File plotfile = new File(dir, "totalStats.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%s.pdf\"\n", title));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s.eps\"\n", title));
		}
		writer.write("set ylabel \"Time(ms)\"\n");
		writer.write("set xlabel \"Tuning Rounds\"\n");
		writer.write(String.format("set title \"%s\"\n", title));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");
		writer.write(String
				.format("plot \"%s\" using 1:6 with linespoints title \"Current best running time\"\n",
						ptotalFile));
		writer.write(String.format(
				"plot \"%s\" using 1:4 with linespoints title \"Run time\"\n",
				ptotalFile));
		writer.write(String
				.format("plot \"%s\" using 1:3 with linespoints title \"Compile time\"\n",
						ptotalFile));
		writer.write(String
				.format("plot \"%s\" using 1:5 with linespoints title \"Drain time\"\n",
						ptotalFile));
		writer.write("set ylabel \"Time(s)\"\n");
		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Tuning Round time\"\n",
						ptotalFile));
		writer.close();
		return plotfile;
	}

	/**
	 * Creates a plot file that uses data from all processed files (
	 * {@link #pRunTimeFile}, {@link #pCompTimeFile}, {@link #pDrainTimeFile}
	 * and {@link #pTuneRoundTimeFile}).
	 */
	private static File createProcessedPlotFile(File dir, String appName)
			throws IOException {
		String title = getTitle(appName);
		boolean pdf = true;
		File plotfile = new File(dir, "processed.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%sP.pdf\"\n", title));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s.eps\"\n", title));
		}
		writer.write("set ylabel \"Time(ms)\"\n");
		writer.write("set xlabel \"Tuning Rounds\"\n");
		writer.write(String.format("set title \"%s\"\n", title));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");
		writer.write(String
				.format("plot \"%s\" using 1:4 with linespoints title \"Current best running time\"\n",
						pRunTimeFile));
		writer.write(String.format(
				"plot \"%s\" using 1:3 with linespoints title \"Run time\"\n",
				pRunTimeFile));
		writer.write(String
				.format("plot \"%s\" using 1:3 with linespoints title \"Compile time\"\n",
						pCompTimeFile));
		writer.write(String
				.format("plot \"%s\" using 1:3 with linespoints title \"Drain time\"\n",
						pDrainTimeFile));
		writer.write("set ylabel \"Time(s)\"\n");
		writer.write(String
				.format("plot \"%s\" using 1:3 with linespoints title \"Tuning Round time\"\n",
						pTuneRoundTimeFile));
		writer.close();
		return plotfile;
	}

	private static File makeHeapPlotFile(File dir, String name,
			String dataFile1, String dataFile2) throws IOException {
		boolean pdf = true;
		File plotfile = new File(dir, "heapplot.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%sHeap.pdf\"\n", name));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%sHeap.eps\"\n", name));
		}
		writer.write("set ylabel \"Memory(MB)\"\n");
		writer.write("set xlabel \"Tuning Rounds\"\n");
		writer.write(String.format("set title \"%sHeap\"\n", name));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");

		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Heap Free Size\","
						+ "\"%s\" using 1:3 with linespoints title \"Heap Size\","
						+ "\"%s\" using 1:4 with linespoints title \"Heap Max Size\" \n",
						dataFile1, dataFile1, dataFile1));
		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Heap Free Size\","
						+ "\"%s\" using 1:3 with linespoints title \"Heap Size\","
						+ "\"%s\" using 1:4 with linespoints title \"Heap Max Size\" \n",
						dataFile2, dataFile2, dataFile2));

		writer.close();
		return plotfile;
	}

	private static void plot(File dir, File plotFile) throws IOException {
		String[] s = { "/usr/bin/gnuplot", plotFile.getName() };
		try {
			ProcessBuilder pb = new ProcessBuilder(s);
			pb.directory(dir);
			Process proc = pb.start();
			InputStream stdin = (InputStream) proc.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null)
				System.err.println("gnuplot:" + line);
			int exitVal = proc.waitFor();
			if (exitVal != 0)
				System.out.println("gnuplot Process exitValue: " + exitVal);
			proc.getInputStream().close();
			proc.getOutputStream().close();
			proc.getErrorStream().close();
		} catch (Exception e) {
			System.err.println("Fail: " + e);
		}
	}

	public static void summarizeHeap(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		File f1 = writeHeapStat(
				String.format("%s%sslurm-662553.out", appName, File.separator),
				summaryDir);
		File f2 = writeHeapStat(
				String.format("%s%sslurm-662554.out", appName, File.separator),
				summaryDir);
		File f = makeHeapPlotFile(summaryDir, appName, f1.getName(),
				f2.getName());
		plot(summaryDir, f);
	}

	public static void GraphPropertyProcessor(String appName)
			throws IOException {
		BufferedReader reader = new BufferedReader(
				new FileReader(String.format("%s%sGraphProperty.txt", appName,
						File.separator)));
		int truecnt = 0;
		int falsecnt = 0;
		String line;
		while ((line = reader.readLine()) != null) {
			if (line.contains("True"))
				truecnt++;
			else if (line.contains("False"))
				falsecnt++;
			else
				System.err.println("Not a tuning line...");
		}
		reader.close();
		double total = truecnt + falsecnt;
		System.out.println(String.format(
				"Total=%f, TrueCount=%d(%f), FalseCount=%d(%f)", total,
				truecnt, (truecnt / total), falsecnt, (falsecnt / total)));
	}

	/**
	 * If you evaluate a same configuration for several times by setting the
	 * {@link Options#evaluationCount} value, then use this method to process
	 * the output evaluation.txt.
	 */
	public static void processEvaltxt(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%sevaluation.txt", appName, File.separator)));
		File outFile = new File(summaryDir, pEvalTimeFile);
		FileWriter writer = new FileWriter(outFile, false);
		String line;
		int i = 0;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("Evaluation")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				String data = String.format("%-6d\t%-6d\n", ++i, val);
				writer.write(data);
			}
		}
		writer.flush();
		reader.close();
		writer.close();
	}

	/**
	 * If you run a set of configurations in a cyclic manner by setting
	 * {@link Options#verificationCount}, then use this method to process the
	 * runTime.txt.
	 */
	private static void processVerifycaionRunTime(String appName, File outDir,
			String cfgName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%srunTime.txt", appName, File.separator)));
		File outFile = new File(outDir, String.format("processed%s.txt",
				cfgName));
		FileWriter writer = new FileWriter(outFile, false);
		String line;
		String cfgPrefix = "Init";
		int i = 0;
		int min = Integer.MAX_VALUE;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("----------------------------"))
				cfgPrefix = cfgString(line);
			if (cfgName.equals(cfgPrefix))
				if (line.startsWith("Execution")) {
					String[] arr = line.split(" ");
					String time = arr[3].trim();
					time = time.substring(0, time.length() - 2);
					int val = Integer.parseInt(time);
					if (val < 1)
						val = 2 * min;
					min = Math.min(min, val);
					String data = String.format("%-6d\t%-6s\t%-6d\t%-6d\n",
							++i, cfgPrefix, val, min);
					writer.write(data);
				}
		}
		writer.flush();
		reader.close();
		writer.close();
	}

	public static void processVerifycaionRun(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		processVerifycaionRunTime(appName, summaryDir,
				"final_NestedSplitJoinCore.cfg");
	}

	private static String getTitle(String appName) {
		String benchmarkName = getBenchmarkName(appName);
		return benchmarkName == null ? appName : benchmarkName;
	}

	private static String getBenchmarkName(String appName) {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(String.format("%s%sREADME.txt",
					appName, File.separator));
			prop.load(input);
		} catch (IOException ex) {
			System.err.println("Failed to load README.txt");
		}
		return prop.getProperty("benchmarkName");
	}
}
