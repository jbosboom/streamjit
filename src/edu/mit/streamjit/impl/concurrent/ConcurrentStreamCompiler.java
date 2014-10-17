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
package edu.mit.streamjit.impl.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.AbstractDrainer.BlobGraph;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;

/**
 * A stream compiler that partitions a streamgraph into multiple blobs and
 * execute it on multiple threads.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 8, 2013
 */
public class ConcurrentStreamCompiler implements StreamCompiler {
	int noOfBlobs;

	/**
	 * @param Patrions
	 *            a stream graph up to noOfBlobs many blobs and executes each
	 *            blob on each thread.
	 */
	public ConcurrentStreamCompiler(int noOfBlobs) {
		if (noOfBlobs < 1)
			throw new IllegalArgumentException(
					"noOfBlobs should be 1 or greater");
		this.noOfBlobs = noOfBlobs;
	}

	public ConcurrentStreamCompiler(Configuration cfg) {

		IntParameter threadCount = cfg.getParameter("threadCount",
				IntParameter.class);
		this.noOfBlobs = threadCount.getValue();
		if (noOfBlobs < 1)
			throw new IllegalArgumentException(
					"noOfBlobs should be 1 or greater");
		this.noOfBlobs = noOfBlobs;
	}

	@Override
	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		Token inputToken = Token.createOverallInputToken(source);
		Token outputToken = Token.createOverallOutputToken(sink);

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> tempList = horzPartitioner.partitionEqually(
				stream, source, sink, this.noOfBlobs);

		List<Set<Worker<?, ?>>> partitionList = new ArrayList<>();
		for (Set<Worker<?, ?>> blob : tempList) {
			if (!blob.isEmpty())
				partitionList.add(blob);
		}

		// TODO: Copied form DebugStreamCompilecollr. Need to be verified for
		// this context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		Set<Blob> blobSet = new HashSet<>();
		for (Set<Worker<?, ?>> partition : partitionList) {
			blobSet.add(new Interpreter(partition, constraints, makeConfig(),
					null));
		}

		BlobGraph bg = new BlobGraph(partitionList);

		Map<Token, Buffer> bufferMap = createBufferMap(blobSet);

		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer inputBuffer = InputBufferFactory.unwrap(input)
				.createReadableBuffer(1000);
		Buffer outputBuffer = OutputBufferFactory.unwrap(output)
				.createWritableBuffer(1000);

		assert !bufferMap.containsKey(inputToken) : "Overall input buffer is already created.";
		assert !bufferMap.containsKey(outputToken) : "Overall output buffer is already created.";

		bufferMap.put(inputToken, inputBuffer);
		bufferMap.put(outputToken, outputBuffer);

		for (Blob b : blobSet) {
			b.installBuffers(bufferMap);
		}

		final ConcurrentCompiledStream cs = new ConcurrentCompiledStream(bg,
				blobSet);

		if (input instanceof ManualInput)
			InputBufferFactory.setManualInputDelegate((ManualInput<I>) input,
					new InputBufferFactory.AbstractManualInputDelegate<I>(
							inputBuffer) {
						@Override
						public void drain() {
							cs.drain();
						}
					});
		else
			cs.drain();
		return cs;
	}

	// TODO: Buffer sizes, including head and tail buffers, must be optimized.
	// consider adding some tuning factor.
	/**
	 * Only create buffers for inter worker communication. No global input or
	 * global output buffer is created.
	 * 
	 * @param blobList
	 * @return
	 */
	private Map<Token, Buffer> createBufferMap(Set<Blob> blobList) {

		Map<Token, Buffer> bufferMap = new HashMap<>();

		Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
		Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

		for (Blob b : blobList) {
			Set<Blob.Token> inputs = b.getInputs();
			for (Token t : inputs) {
				minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}

			Set<Blob.Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}
		}

		for (Token t : minInputBufCapaciy.keySet()) {
			int bufSize;
			if (minOutputBufCapaciy.containsKey(t)) {
				bufSize = lcm(minInputBufCapaciy.get(t),
						minOutputBufCapaciy.get(t));

				// TODO: Just to increase the performance. Change it later
				bufSize = Math.max(1000, bufSize);

				Buffer buf = new ConcurrentArrayBuffer(bufSize);
				bufferMap.put(t, buf);
			}
		}
		return bufferMap;
	}

	private int gcd(int a, int b) {
		while (true) {
			if (a == 0)
				return b;
			b %= a;
			if (b == 0)
				return a;
			a %= b;
		}
	}

	private int lcm(int a, int b) {
		int val = gcd(a, b);
		return val != 0 ? ((a * b) / val) : 0;
	}

	public static class ConcurrentCompiledStream implements CompiledStream {

		private Map<Blob, Set<BlobThread>> threadMap = new HashMap<>();
		private final AbstractDrainer drainer;

		public ConcurrentCompiledStream(BlobGraph blobGraph, Set<Blob> blobSet) {
			List<Thread> blobThreads = new ArrayList<>(blobSet.size());
			for (final Blob b : blobSet) {
				BlobThread t = new BlobThread(b.getCoreCode(0));
				blobThreads.add(t);
				threadMap.put(b, Collections.singleton(t));
			}
			this.drainer = new ConcurrentDrainer(blobGraph, threadMap);
			start(blobThreads);
		}

		/*
		 * If CompiledStream provides start() interface function then make this
		 * public. Currently start() is called inside the
		 * ConcurrentCompiledStream's constructor.
		 */
		private void start(List<Thread> blobThreads) {
			for (Thread t : blobThreads) {
				t.start();
			}
		}

		protected void drain() {
			drainer.startDraining(2);
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

	private static Configuration makeConfig() {
		List<ChannelFactory> universe = Arrays
				.<ChannelFactory> asList(new ConcurrentChannelFactory());
		Configuration config = Configuration
				.builder()
				.addParameter(
						new SwitchParameter<>("channelFactory",
								ChannelFactory.class, universe.get(0), universe))
				.build();
		return config;
	}
}