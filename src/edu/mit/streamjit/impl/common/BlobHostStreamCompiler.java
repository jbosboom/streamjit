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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
		Blob blob = blobFactory.makeBlob(workers, getConfiguration(workers), getMaxNumCores(), null);

		Token inputToken = Iterables.getOnlyElement(blob.getInputs());
		Token outputToken = Iterables.getOnlyElement(blob.getOutputs());
		Buffer inputBuffer = InputBufferFactory.unwrap(input).createReadableBuffer(blob.getMinimumBufferCapacity(inputToken));
		Buffer outputBuffer = OutputBufferFactory.unwrap(output).createWritableBuffer(blob.getMinimumBufferCapacity(outputToken));
		ImmutableMap<Token, Buffer> bufferMap = ImmutableMap.<Token, Buffer>builder()
				.put(inputToken, inputBuffer)
				.put(outputToken, outputBuffer)
				.build();
		blob.installBuffers(bufferMap);

		ImmutableList.Builder<PollingCoreThread> threadsBuilder = ImmutableList.builder();
		for (int i = 0; i < blob.getCoreCount(); ++i) {
			PollingCoreThread thread = new PollingCoreThread(blob.getCoreCode(i), blob.toString()+"-"+i);
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

	/**
	 * Get a configuration for the given worker.  This implementation returns
	 * the default configuration from the blob factory, but subclasses can
	 * override this to implement compiler-specific options.
	 * @param workers the set of workers in the blob
	 * @return a configuration
	 */
	protected Configuration getConfiguration(Set<Worker<?, ?>> workers) {
		return blobFactory.getDefaultConfiguration(workers);
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
			blob.drain(new Runnable() {
				@Override
				public void run() {
					for (PollingCoreThread t : threads)
						t.requestStop();
				}
			});
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
		private final Runnable coreCode;
		private volatile boolean running = true;
		private volatile CountDownLatch latch;
		private PollingCoreThread(Runnable target, String name) {
			super(name);
			this.coreCode = target;
		}
		@Override
		public void run() {
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
