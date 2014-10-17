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
package edu.mit.streamjit.impl.distributed;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.impl.distributed.runtimer.DistributedDrainer;
import edu.mit.streamjit.impl.distributed.runtimer.OnlineTuner;
import edu.mit.streamjit.impl.distributed.HeadChannel.HeadBuffer;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;

/**
 * 
 * The OneToOneElement that is asked to compile by this {@link Compiler} must be
 * unique. Compilation will fail if default subtypes of the
 * {@link OneToOneElement}s such as {@link Pipeline}, {@link Splitjoin} and etc
 * are be passed.
 * <p>
 * TODO: {@link DistributedStreamCompiler} must work with 1 {@link StreamNode}
 * as well. In that case, it should behave like a
 * {@link ConcurrentStreamCompiler}.
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	/**
	 * Configuration from Opentuner.
	 */
	Configuration cfg;

	/**
	 * Total number of nodes including controller node.
	 */
	int noOfnodes;

	/**
	 * @param noOfnodes
	 *            : Total number of nodes the stream application intended to run
	 *            - including controller node. If it is 1 then it means the
	 *            whole stream application is supposed to run on controller.
	 */
	public DistributedStreamCompiler(int noOfnodes) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		this.noOfnodes = noOfnodes;
	}

	/**
	 * Run the whole application on the controller node.
	 */
	public DistributedStreamCompiler() {
		this(1);
	}

	/**
	 * Run the application with the passed configureation.
	 */
	public DistributedStreamCompiler(int noOfnodes, Configuration cfg) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		this.noOfnodes = noOfnodes;
		this.cfg = cfg;
	}

	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {

		checkforDefaultOneToOneElement(stream);

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		Map<CommunicationType, Integer> conTypeCount = new HashMap<>();
		// conTypeCount.put(CommunicationType.LOCAL, 1);
		conTypeCount.put(CommunicationType.TCP, this.noOfnodes);
		Controller controller = new Controller();
		controller.connect(conTypeCount);

		StreamJitApp app = new StreamJitApp(stream, source, sink);
		ConfigurationManager cfgManager = new HotSpotTuning(app);
		BlobFactory bf = new DistributedBlobFactory(cfgManager, noOfnodes);
		this.cfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(source));

		if (GlobalConstants.tune) {

		} else {
			this.cfg = readConfiguration(stream.getClass().getSimpleName());
		}

		if (cfg == null) {
			System.err
					.println("Configuration is null. Runs the app with horizontal partitioning.");
			Integer[] machineIds = new Integer[this.noOfnodes];
			for (int i = 0; i < machineIds.length; i++) {
				machineIds[i] = i + 1;
			}
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
					machineIds, stream, source, sink);
			app.newPartitionMap(partitionsMachineMap);
		} else
			cfgManager.newConfiguration(cfg);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		StreamJitAppManager manager = new StreamJitAppManager(controller, app,
				cfgManager);
		final AbstractDrainer drainer = new DistributedDrainer(manager);
		drainer.setBlobGraph(app.blobGraph);

		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer head = InputBufferFactory.unwrap(input).createReadableBuffer(
				1000);
		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				1000);

		boolean needTermination;

		if (input instanceof ManualInput) {
			needTermination = false;
			InputBufferFactory
					.setManualInputDelegate(
							(ManualInput<I>) input,
							new InputBufferFactory.AbstractManualInputDelegate<I>(
									head) {
								@Override
								public void drain() {
									drainer.startDraining(2);
								}
							});
		} else {
			needTermination = true;
			head = new HeadBuffer(head, drainer);
		}

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder.put(Token.createOverallInputToken(source), head);
		bufferMapBuilder.put(Token.createOverallOutputToken(sink), tail);

		app.bufferMap = bufferMapBuilder.build();
		app.constraints = constraints;

		manager.reconfigure();
		CompiledStream cs = new DistributedCompiledStream(drainer);

		if (GlobalConstants.tune && this.cfg != null) {
			OnlineTuner tuner = new OnlineTuner(drainer, manager, app,
					cfgManager, needTermination);
			new Thread(tuner, "OnlineTuner").start();
		}
		return cs;
	}

	private Configuration readConfiguration(String simpeName) {
		String name = String.format("%s.cfg", simpeName);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(name));
			String json = reader.readLine();
			reader.close();
			return Configuration.fromJson(json);
		} catch (Exception ex) {
			System.err.println(String.format(
					"File reader error. No %s configuration file.", name));
		}
		return null;
	}

	private <I, O> Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Integer[] machineIds, OneToOneElement<I, O> stream,
			Worker<I, ?> source, Worker<?, O> sink) {
		int totalCores = machineIds.length;

		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner
				.partitionEqually(stream, source, sink, totalCores);

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		for (Integer machineID : machineIds) {
			partitionsMachineMap.put(machineID,
					new ArrayList<Set<Worker<?, ?>>>());
		}

		int index = 0;
		while (index < partitionList.size()) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(
						partitionList.get(index++));
			}
		}
		return partitionsMachineMap;
	}

	/**
	 * TODO: Need to check for other default subtypes of {@link OneToOneElement}
	 * s. Now only checks for first generation children.
	 * 
	 * @param stream
	 * @throws StreamCompilationFailedException
	 *             if stream is default subtype of OneToOneElement
	 */
	private <I, O> void checkforDefaultOneToOneElement(
			OneToOneElement<I, O> stream) {

		if (stream.getClass() == Pipeline.class
				|| stream.getClass() == Splitjoin.class
				|| stream.getClass() == Filter.class) {
			throw new StreamCompilationFailedException(
					"Default subtypes of OneToOneElement are not accepted for compilation by this compiler. OneToOneElement that passed should be unique");
		}
	}

	private static class DistributedCompiledStream implements CompiledStream {

		AbstractDrainer drainer;

		public DistributedCompiledStream(AbstractDrainer drainer) {
			this.drainer = drainer;
		}

		@Override
		public boolean isDrained() {
			return drainer.isDrained();
		}

		@Override
		public void awaitDrained() throws InterruptedException {
			drainer.awaitDrained();

		}

		@Override
		public void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			drainer.awaitDrained(timeout, unit);
		}
	}
}