package edu.mit.streamjit.impl.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.google.common.base.Splitter;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.DistributedBlobFactory;
import edu.mit.streamjit.impl.distributed.HotSpotTuning;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.json.Jsonifiers;

public class ConfigurationEditor {

	static String name;
	static int noofwrks;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// generate1(new ChannelVocoder7.ChannelVocoder7Kernel(), 16);
		// edit1(name, noofwrks);
		// print("4366NestedSplitJoinCore.cfg");
		// convert();
		// changeMultiplierVal();
	}

	/**
	 * Reads a configuration and changes its multiplier value.
	 */
	private static void changeMultiplierVal() {
		String appName = "NestedSplitJoinCore";
		String cfgFileName = "Final_NestedSplitJoinCore.cfg";
		Configuration config = ConfigurationUtils.readConfiguration(String
				.format("%s%sconfigurations%s%s", appName, File.separator,
						File.separator, cfgFileName));
		Configuration.Builder builder = Configuration.builder(config);
		IntParameter mulParam = config.getParameter("multiplier",
				IntParameter.class);
		if (mulParam != null) {
			System.out.println("Multiplier values is " + mulParam.getValue());
			builder.removeParameter(mulParam.getName());
		}

		IntParameter newMulParam = new IntParameter("multiplier", 1, 100, 100);
		builder.addParameter(newMulParam);
		ConfigurationUtils.saveConfg(builder.build(), "444", appName);

	}

	private static void generate(OneToOneElement<?, ?> stream, int noOfnodes) {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<?, ?> source = (Worker<?, ?>) primitiveConnector.getSource();
		Worker<?, ?> sink = (Worker<?, ?>) primitiveConnector.getSink();
		noofwrks = Workers.getIdentifier(sink) + 1;

		BlobFactory bf = new DistributedBlobFactory(noOfnodes);
		Configuration cfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(source));

		String appName = stream.getClass().getSimpleName();

		name = String.format("%s%sconfigurations%shand_%s.cfg", appName,
				File.separator, File.separator, appName);

		try {
			FileWriter writer = new FileWriter(name, false);
			writer.write(cfg.toJson());
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void edit(String name, int maxWor)
			throws NumberFormatException, IOException {
		Configuration cfg;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(name));
			String json = reader.readLine();
			cfg = Configuration.fromJson(json);
			reader.close();
		} catch (Exception ex) {
			System.err.println("File reader error");
			return;
		}

		Configuration.Builder builder = Configuration.builder(cfg);
		BufferedReader keyinreader = new BufferedReader(new InputStreamReader(
				System.in));

		for (int i = 0; i < maxWor; i++) {
			String s = String.format("worker%dtomachine", i);
			SwitchParameter<Integer> p = (SwitchParameter<Integer>) cfg
					.getParameter(s);
			System.out.println(p.getName() + " - " + p.getValue());
			int val = Integer.parseInt(keyinreader.readLine());
			builder.removeParameter(s);
			builder.addParameter(new SwitchParameter<Integer>(s, Integer.class,
					val, p.getUniverse()));
		}

		cfg = builder.build();
		FileWriter writer = new FileWriter(name);
		writer.write(cfg.toJson());
		writer.close();
		System.out.println("Successfully updated");
	}

	private static void generate1(OneToOneElement<?, ?> stream, int noOfnodes) {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<?, ?> source = (Worker<?, ?>) primitiveConnector.getSource();
		Worker<?, ?> sink = (Worker<?, ?>) primitiveConnector.getSink();
		noofwrks = Workers.getIdentifier(sink) + 1;

		StreamJitApp app = new StreamJitApp(stream, source, sink);
		ConfigurationManager cfgManager = new HotSpotTuning(app);
		BlobFactory bf = new DistributedBlobFactory(cfgManager, noOfnodes);

		Configuration cfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(source));

		String appName = stream.getClass().getSimpleName();

		name = String.format("%s%sconfigurations%shand_%s.cfg", appName,
				File.separator, File.separator, appName);

		try {
			FileWriter writer = new FileWriter(name, false);
			writer.write(cfg.toJson());
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Generates default cfg of {@link Compiler2BlobFactory}. No modification
	 * done.
	 * 
	 * @param stream
	 */
	private static void generate2(OneToOneElement<?, ?> stream) {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<?, ?> source = (Worker<?, ?>) primitiveConnector.getSource();
		BlobFactory bf = new Compiler2BlobFactory();
		// BlobFactory bf = new DistributedBlobFactory(1);

		Configuration cfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(source));

		name = String.format("hand_%s.cfg", stream.getClass().getSimpleName());

		ConfigurationUtils.saveConfg(cfg, "0", name);
	}

	private static void edit1(String name, int maxWor)
			throws NumberFormatException, IOException {
		Configuration cfg;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(name));
			String json = reader.readLine();
			cfg = Configuration.fromJson(json);
			reader.close();
		} catch (Exception ex) {
			System.err.println("File reader error");
			return;
		}

		Configuration.Builder builder = Configuration.builder(cfg);
		BufferedReader keyinreader = new BufferedReader(new InputStreamReader(
				System.in));

		for (int i = 0; i < maxWor; i++) {
			String wrkrMachineName = String.format("worker%dtomachine", i);
			String wrkrCutname = String.format("worker%dcut", i);

			SwitchParameter<Integer> wrkrMachine = cfg.getParameter(
					wrkrMachineName, SwitchParameter.class);
			IntParameter wrkrCut = cfg.getParameter(wrkrCutname,
					IntParameter.class);

			if (wrkrMachine != null) {
				System.out.println(wrkrMachine.toString());
				boolean isOk1 = false;
				while (!isOk1) {
					try {
						int val = Integer.parseInt(keyinreader.readLine());
						builder.removeParameter(wrkrMachine.getName());
						builder.addParameter(new SwitchParameter<Integer>(
								wrkrMachine.getName(), Integer.class, val,
								wrkrMachine.getUniverse()));
						isOk1 = true;
					} catch (Exception ex) {
						ex.printStackTrace();
						isOk1 = false;
					}
				}
			}

			if (wrkrCut != null) {
				System.out.println(wrkrCut.toString());
				boolean isOk = false;
				while (!isOk) {
					try {
						int val = Integer.parseInt(keyinreader.readLine());
						builder.removeParameter(wrkrCut.getName());
						builder.addParameter(new IntParameter(
								wrkrCut.getName(), wrkrCut.getRange(), val));
						isOk = true;
					} catch (Exception ex) {
						ex.printStackTrace();
						isOk = false;
					}
				}
			}
		}

		cfg = builder.build();
		FileWriter writer = new FileWriter(name);
		writer.write(cfg.toJson());
		writer.close();
		System.out.println("Successfully updated");
	}

	private static void print(String name) {
		Configuration cfg;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(name));
			String json = reader.readLine();
			cfg = Configuration.fromJson(json);
			reader.close();
		} catch (Exception ex) {
			System.err.println("File reader error");
			return;
		}

		for (Map.Entry<String, Parameter> en : cfg.getParametersMap()
				.entrySet()) {
			if (en.getValue() instanceof SwitchParameter<?>) {
				SwitchParameter<Integer> sp = (SwitchParameter<Integer>) en
						.getValue();
				System.out.println(sp.getName() + " - " + sp.getValue());
			}
		}
	}

	private static void convert() {
		String appName = "ChannelVocoder7Kernel";
		Configuration cfg = ConfigurationUtils.readConfiguration(String.format(
				"%s%sconfigurations%s%d%s.cfg", appName, File.separator,
				File.separator, 1, appName));
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					String.format("%d%s.cfg", 0, appName)));
			String pythonDict = reader.readLine();
			reader.close();

			Configuration finalCfg = rebuildConfiguration(pythonDict, cfg);
			ConfigurationUtils.saveConfg(finalCfg, "0", appName);
		} catch (Exception ex) {
			ex.printStackTrace();
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
	private static Configuration rebuildConfiguration(String pythonDict,
			Configuration config) {
		// System.out.println(pythonDict);
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

	/**
	 * TODO: This method is totally unnecessary if we remove the usage of the
	 * name "class" in side {@link Configuration}.
	 * 
	 * @param cfg
	 * @return
	 */
	private static String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}
}
