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
package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.CompilerBlobFactory;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.apps.bitonicsort.BitonicSort;
import edu.mit.streamjit.test.apps.channelvocoder7.ChannelVocoder7;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * Offline tuner tunes a StreamJit app in a Start-Stop-Restart manner.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Aug 20, 2013
 */
public class OfflineTuner {

	OpenTuner autoTuner;

	public OfflineTuner() {
		autoTuner = new TCPTuner();
	}

	private String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}

	private double runApp(Benchmark app, Configuration cfg)
			throws InterruptedException {
		StreamCompiler sc;

		// SwitchParameter<Integer> compiler = cfg.getParameter("compiler",
		// SwitchParameter.class, Integer.class);
		// if (compiler.getValue() == 0) {
		// CompilerStreamCompiler csc = new CompilerStreamCompiler();
		// csc.setConfig(cfg);
		// sc = csc;
		// } else
		// sc = new ConcurrentStreamCompiler(cfg);

		CompilerStreamCompiler csc = new CompilerStreamCompiler();
		csc.setConfig(cfg);
		sc = csc;

		long startTime = System.nanoTime();

		Dataset dataset = app.inputs().get(0);

		// Input<Object> input = dataset.input();
		Input<Object> input = Datasets.nCopies(100, dataset.input());
		Output<Object> output = Output.blackHole();

		run(sc, app.instantiate(), input, output);
		long endTime = System.nanoTime();
		double diff = (endTime - startTime) / 1e6;
		return diff;
	}

	public void tune(Benchmark app) throws InterruptedException {
		int tryCount = 0;
		try {
			autoTuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit.py", File.separator,
					File.separator, File.separator));

			autoTuner.writeLine("program");
			autoTuner.writeLine(app.toString());
			File file = new File(app.toString() + ".txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			// TODO: BlobFactory.getDefaultConfiguration() asks for workers. But
			// from outside workers are not available. need to do something
			// else. This is not a proper design to do.
			BlobFactory bf = new CompilerBlobFactory();
			ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
			OneToOneElement<?, ?> stream = app.instantiate();
			stream.visit(cwv);
			ImmutableSet<Worker<?, ?>> workers = Workers
					.getAllWorkersInGraph(cwv.getSource());
			Configuration cfg = bf.getDefaultConfiguration(workers);
			// Builder builer = Configuration.builder(cfg);

			// List<Integer> compilers = new ArrayList<>();
			// compilers.add(0);
			// compilers.add(1);
			// builer.addParameter(new SwitchParameter<Integer>("compiler",
			// Integer.class, compilers.get(0), compilers));

			// Builder builer1 = Configuration.builder();
			// builer1.addParameter(new IntParameter("threadCount", 1, 10, 1));
			// Configuration newConfg = builer1.build();
			autoTuner.writeLine("confg");
			String s = getConfigurationString(cfg);
			autoTuner.writeLine(s);

			double minRuntime = Double.MAX_VALUE;

			bw.write("\nNew tune run.............\n");
			while (true) {
				String pythonDict = autoTuner.readLine();
				if (pythonDict.equals("Completed")) {
					String finalConfg = autoTuner.readLine();
					printFinalConfg(finalConfg, bw);
					bw.close();
					break;
				}

				System.out
						.println("----------------------------------------------");
				System.out.println(tryCount++);
				Configuration config = rebuildConfiguration(pythonDict, cfg);
				try {
					double time = runApp(app, config);
					System.out.println("Execution time is " + time
							+ " milli seconds");
					autoTuner.writeLine(new Double(time).toString());
					minRuntime = Math.min(minRuntime, time);
					bw.write("----------------------------------------------\n");
					bw.write(new Integer(tryCount).toString());
					bw.write(" - ");
					bw.write(new Double(minRuntime).toString());
					bw.write(" - ");
					bw.write(new Double(time).toString());
					bw.write("\n");
					bw.flush();

				} catch (Exception ex) {
					System.err
							.println("Couldn't compile the stream graph with this configuration");
					autoTuner.writeLine(new Double(Double.POSITIVE_INFINITY)
							.toString());
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void printFinalConfg(String finalConfg, BufferedWriter bw)
			throws IOException {

		finalConfg = finalConfg.replaceAll("u'", "");
		finalConfg = finalConfg.replaceAll("':", "");
		finalConfg = finalConfg.replaceAll("\\{", "");
		finalConfg = finalConfg.replaceAll("\\}", "");
		Splitter dictSplitter = Splitter.on(", ").omitEmptyStrings()
				.trimResults();
		System.out.println("********************************");
		System.out.println("This is the final configuration*");
		bw.write("\n********************************\n");
		bw.write("This is the final configuration\n");

		for (String s : dictSplitter.split(finalConfg)) {
			String[] str = s.split(" ");
			if (str.length != 2)
				throw new AssertionError("Wrong python dictionary...");
			// System.out.println(String.format("\t%s = %s", str[0], str[1]));
			bw.write(String.format("\t%s = %s\n", str[0], str[1]));
		}
	}

	/**
	 * Creates a new {@link Configuration} from the received python dictionary
	 * string. This is not a good way to do.
	 * <p>
	 * TODO: Need to add a method to {@link Configuration} so that the
	 * configuration object can be updated from the python dict string. Now we
	 * are destructing the old confg object and recreating a new one every time.
	 * Not a appreciatable way.
	 *
	 * @param pythonDict
	 *            Python dictionary string. Autotuner gives a dictionary of
	 *            features with trial values.
	 * @param config
	 *            Old configuration object.
	 * @return New configuration object with updated values from the pythonDict.
	 */
	private Configuration rebuildConfiguration(String pythonDict,
			Configuration config) {
		// System.out.println(pythonDict);
		checkNotNull(pythonDict, "Received Python dictionary is null");
		pythonDict = pythonDict.replaceAll("u'", "");
		pythonDict = pythonDict.replaceAll("':", "");
		pythonDict = pythonDict.replaceAll("\\{", "");
		pythonDict = pythonDict.replaceAll("\\}", "");
		Splitter dictSplitter = Splitter.on(", ").omitEmptyStrings()
				.trimResults();
		Configuration.Builder builder = Configuration.builder();
		System.out.println("New parameter values from Opentuner...");
		for (String s : dictSplitter.split(pythonDict)) {
			String[] str = s.split(" ");
			if (str.length != 2)
				throw new AssertionError("Wrong python dictionary...");
			Parameter p = config.getParameter(str[0]);
			if (p == null)
				continue;
			// System.out.println(String.format("\t%s = %s", str[0], str[1]));
			if (p instanceof IntParameter) {
				IntParameter ip = (IntParameter) p;
				builder.addParameter(new IntParameter(ip.getName(),
						ip.getMin(), ip.getMax(), Integer.parseInt(str[1])));

			} else if (p instanceof SwitchParameter<?>) {
				SwitchParameter sp = (SwitchParameter) p;
				Class<?> type = sp.getGenericParameter();
				int val = Integer.parseInt(str[1]);
				SwitchParameter<?> sp1 = new SwitchParameter(sp.getName(),
						type, sp.getUniverse().get(val), sp.getUniverse());
				builder.addParameter(sp1);
			}

		}
		return builder.build();
	}

	private void run(StreamCompiler compiler,
			OneToOneElement<Object, Object> streamGraph, Input<Object> input,
			Output<Object> output) {
		System.out.println("Running the StreamJit application...");
		CompiledStream stream = compiler.compile(streamGraph, input, output);
		try {
			stream.awaitDrained();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 *            [0] - String topLevelWorkerName args[1] - String jarFilePath
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {

		BenchmarkProvider provider = new ChannelVocoder7();
		// BenchmarkProvider provider = new FMRadio.FMRadioBenchmarkProvider();
		// BenchmarkProvider provider = new BitonicSort();
		// BenchmarkProvider provider = new FileInputSanity();
		// BenchmarkProvider provider = new SplitjoinOrderSanity();
		// BenchmarkProvider provider = new HelperFunctionSanity();

		Benchmark benchmark = provider.iterator().next();

		OfflineTuner tuner = new OfflineTuner();
		tuner.tune(benchmark);
	}
}
