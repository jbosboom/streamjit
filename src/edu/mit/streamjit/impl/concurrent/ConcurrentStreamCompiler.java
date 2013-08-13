package edu.mit.streamjit.impl.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.common.BlobGraph;
import edu.mit.streamjit.impl.common.BlobGraph.AbstractDrainer;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.interp.AbstractCompiledStream;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;
import java.util.Collections;

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

	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

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
		// this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		Set<Blob> blobSet = new HashSet<>();
		for (Set<Worker<?, ?>> partition : partitionList) {
			blobSet.add(new Interpreter(partition, constraints, makeConfig()));
		}

		BlobGraph bg = new BlobGraph(partitionList);

		ImmutableMap<Token, Buffer> bufferMap = createBufferMap(blobSet);

		for (Blob b : blobSet) {
			b.installBuffers(bufferMap);
		}

		Buffer head = bufferMap.get(Token.createOverallInputToken(source));
		Buffer tail = bufferMap.get(Token.createOverallOutputToken(sink));
		return new ConcurrentCompiledStream<>(head, tail, bg, blobSet);
	}

	// TODO: Buffer sizes, including head and tail buffers, must be optimized.
	// consider adding some tuning factor
	private ImmutableMap<Token, Buffer> createBufferMap(Set<Blob> blobList) {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

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
			if (minOutputBufCapaciy.containsKey(t))
				bufSize = lcm(minInputBufCapaciy.get(t),
						minOutputBufCapaciy.get(t));
			else
				bufSize = minInputBufCapaciy.get(t);

			// TODO: Just to increase the performance. Change it later
			bufSize = Math.max(1000, bufSize);

			Buffer buf = new ConcurrentArrayBuffer(bufSize);
			bufferMapBuilder.put(t, buf);

			minOutputBufCapaciy.remove(t);
		}

		// Now only overalloutput token should be at minOutputBufCapaciy map
		assert minOutputBufCapaciy.size() == 1 : "Miss match in total input tokens and the output tokens";
		Token outputToken = Iterables.getOnlyElement(minOutputBufCapaciy
				.keySet());
		int bufSize = minOutputBufCapaciy.get(outputToken);

		// TODO: Just to increase the performance. Change it later
		bufSize = Math.max(1000, bufSize);

		Buffer buf = new ConcurrentArrayBuffer(bufSize);
		bufferMapBuilder.put(outputToken, buf);
		return bufferMapBuilder.build();
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

	public static class ConcurrentCompiledStream<I, O> extends
			AbstractCompiledStream<I, O> {

		private Map<Blob, Set<BlobThread>> threadMap = new HashMap<>();
		private final AbstractDrainer drainer;

		public ConcurrentCompiledStream(Buffer inputBuffer,
				Buffer outputBuffer, BlobGraph blobGraph, Set<Blob> blobSet) {
			super(inputBuffer, outputBuffer);
			List<Thread> blobThreads = new ArrayList<>(blobSet.size());
			for (final Blob b : blobSet) {
				BlobThread t = new BlobThread(b.getCoreCode(0));
				blobThreads.add(t);
				threadMap.put(b, Collections.singleton(t));
			}
			this.drainer = new ConcurrentDrainer(blobGraph, false, threadMap);
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

		@Override
		protected void doDrain() {
			drainer.startDraining();
		}

		@Override
		public boolean isDrained() {
			return drainer.isDrained();
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