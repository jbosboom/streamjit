/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.util.affinity.Affinity;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A StreamCompiler that uses a BlobFactory to make a Blob for the entire graph.
 *
 * TODO: there should be a way to specify a custom configuration, but the blob's
 * default config depends on the workers (e.g., the compiler has parameters for
 * each worker to decide if that worker should be fused).  We'll need some
 * notion of a configuration override, or possibly split
 * Blob.getDefaultConfiguration() into worker-specific and worker-independent
 * parts.
 *
 * TODO: should this be in the blob package?
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/27/2013
 */
public class BlobHostStreamCompiler implements StreamCompiler {
	private final BlobFactory blobFactory;
	public BlobHostStreamCompiler(BlobFactory blobFactory) {
		this.blobFactory = blobFactory;
	}

	@Override
	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream, Input<I> input, Output<O> output) {
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		stream.visit(cwv);
		ImmutableSet<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());
		Configuration config = getConfiguration(workers);
		Blob blob = makeBlob(workers, config, input, output);

		Token inputToken = Iterables.getOnlyElement(blob.getInputs());
		Token outputToken = Iterables.getOnlyElement(blob.getOutputs());
		Buffer inputBuffer = makeInputBuffer(input, blob.getMinimumBufferCapacity(inputToken));
		Buffer outputBuffer = makeOutputBuffer(output, blob.getMinimumBufferCapacity(outputToken));
		ImmutableMap.Builder<Token, Buffer> bufferMap = ImmutableMap.<Token, Buffer>builder();
		if (inputBuffer != null)
			bufferMap.put(inputToken, inputBuffer);
		if (outputBuffer != null)
			bufferMap.put(outputToken, outputBuffer);
		blob.installBuffers(bufferMap.build());

		Configuration.PermutationParameter<Integer> affinityParam = config.getParameter("$affinity", Configuration.PermutationParameter.class, Integer.class);
		ImmutableList<? extends Integer> affinityList;
		affinityList = affinityParam != null ? affinityParam.getUniverse() : ImmutableList.copyOf(Affinity.getMaximalAffinity());
		ImmutableList.Builder<PollingCoreThread> threadsBuilder = ImmutableList.builder();
		for (int i = 0; i < blob.getCoreCount(); ++i) {
			PollingCoreThread thread = new PollingCoreThread(affinityList.get(i % affinityList.size()), blob.getCoreCode(i), blob.toString()+"-"+i);
			threadsBuilder.add(thread);
		}
		ImmutableList<PollingCoreThread> threads = threadsBuilder.build();

		final BlobHostCompiledStream cs = new BlobHostCompiledStream(blob, threads);
		if (input instanceof ManualInput)
			InputBufferFactory.setManualInputDelegate((ManualInput<I>)input, new InputBufferFactory.AbstractManualInputDelegate<I>(inputBuffer) {
				@Override
				public void drain() {
					cs.drain();
				}
			});
		else //Input provides all input, so immediately begin to drain.
			cs.drain();

		for (Thread t : threads)
			t.start();
		return cs;
	}

	protected Blob makeBlob(ImmutableSet<Worker<?, ?>> workers, Configuration configuration, Input<?> input, Output<?> output) {
		return blobFactory.makeBlob(workers, configuration, getMaxNumCores(), null);
	}

	/**
	 * Creates a Buffer for the overall input, which will be included in the map
	 * passed to {@link Blob#installBuffers()}. If null is returned, no overall
	 * input buffer will be installed.
	 * @param input the input
	 * @param minCapacity the minimum capacity returned by
	 * {@link Blob#getMinimumBufferCapacity(Blob.Token)}.
	 * @return a Buffer, or null
	 */
	protected Buffer makeInputBuffer(Input<?> input, int minCapacity) {
		return InputBufferFactory.unwrap(input).createReadableBuffer(minCapacity);
	}

	/**
	 * Creates a Buffer for the overall output, which will be included in the
	 * map passed to {@link Blob#installBuffers()}. If null is returned, no
	 * overall output buffer will be installed.
	 * @param output the output
	 * @param minCapacity the minimum capacity returned by
	 * {@link Blob#getMinimumBufferCapacity(Blob.Token)}.
	 * @return a Buffer, or null
	 */
	protected Buffer makeOutputBuffer(Output<?> output, int minCapacity) {
		return OutputBufferFactory.unwrap(output).createWritableBuffer(minCapacity);
	}

	/**
	 * Get a configuration for the given worker.  This implementation returns
	 * the default configuration from the blob factory, but subclasses can
	 * override this to implement compiler-specific options.
	 * @param workers the set of workers in the blob
	 * @return a configuration
	 */
	protected Configuration getConfiguration(Set<Worker<?, ?>> workers) {
		Configuration.Builder builder = Configuration.builder(blobFactory.getDefaultConfiguration(workers));
		return builder.build();
	}

	protected int getMaxNumCores() {
		return 1;
	}

	private static final class BlobHostCompiledStream implements CompiledStream {
		private final Blob blob;
		private final ImmutableList<PollingCoreThread> threads;
		private final CountDownLatch latch;
		private BlobHostCompiledStream(Blob blob, ImmutableList<PollingCoreThread> threads) {
			this.blob = blob;
			this.threads = threads;
			this.latch = new CountDownLatch(this.threads.size());
			for (PollingCoreThread t : this.threads)
				t.latch = this.latch;
		}

		private void drain() {
			blob.drain(() -> threads.forEach(PollingCoreThread::requestStop));
		}

		@Override
		public boolean isDrained() {
			for (Thread t : threads)
				if (t.isAlive())
					return false;
			return true;
		}

		public void awaitDrained() throws InterruptedException {
			latch.await();
		}

		public void awaitDrained(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
			if (!latch.await(timeout, unit))
				throw new TimeoutException();
		}
	}

	private static final class PollingCoreThread extends Thread {
		private final int cpu;
		private final Runnable coreCode;
		private volatile boolean running = true;
		private volatile CountDownLatch latch;
		private PollingCoreThread(int cpu, Runnable target, String name) {
			super(name);
			this.cpu = cpu;
			this.coreCode = target;
		}
		@Override
		public void run() {
			Affinity.setThreadAffinity(ImmutableSet.of(cpu));
			try {
				while (running)
					coreCode.run();
			} finally {
				//Whether we terminated normally or exceptionally, we need to
				//count down so waiting threads don't get stuck.
				latch.countDown();
			}
		}
		public void requestStop() {
			running = false;
		}
	}
}
