package edu.mit.streamjit.impl.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.distributed.DistributedBlobFactory;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioCore;

public class ConfigurationEditor {

	static String name;
	static int noofwrks;
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		generate(new FMRadioCore());
		edit(name, noofwrks);
	}

	private static void generate(OneToOneElement<?, ?> stream) {
		int noOfnodes = 4;

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<?, ?> source = (Worker<?, ?>) primitiveConnector.getSource();
		Worker<?, ?> sink = (Worker<?, ?>) primitiveConnector.getSink();
		noofwrks = Workers.getIdentifier(sink) + 1;

		BlobFactory bf = new DistributedBlobFactory(noOfnodes);
		Configuration cfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(source));

		name = String.format("%s.cfg", stream.getClass().getSimpleName());

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
			IntParameter p = (IntParameter) cfg.getParameter(s);
			System.out.println(p.getName() + " - " + p.getValue());
			int val = Integer.parseInt(keyinreader.readLine());

			builder.removeParameter(s);
			builder.addParameter(new IntParameter(s, p.getMin(), p.getMax(),
					val));
		}

		cfg = builder.build();
		FileWriter writer = new FileWriter(name);
		writer.write(cfg.toJson());
		writer.close();
	}
}
