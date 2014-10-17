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
package edu.mit.streamjit.impl.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.DistributedBlobFactory;

public class ConfigurationEditor {

	static String name;
	static int noofwrks;
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		edit(name, noofwrks);
		// print("4366NestedSplitJoinCore.cfg");
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
}
