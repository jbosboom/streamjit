package edu.mit.streamjit.impl.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

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
	private final int maxNumCores;
	public BlobHostStreamCompiler(BlobFactory blobFactory, int maxNumCores) {
		this.blobFactory = blobFactory;
		this.maxNumCores = maxNumCores;
	}

	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		stream.visit(cwv);
		ImmutableSet<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());
		Blob blob = blobFactory.makeBlob(workers, getConfiguration(workers), maxNumCores);

		Token inputToken = Iterables.getOnlyElement(blob.getInputs());
		Token outputToken = Iterables.getOnlyElement(blob.getOutputs());
		Buffer inputBuffer = Buffers.blockingQueueBuffer(new ArrayBlockingQueue<>(blob.getMinimumBufferCapacity(inputToken)), false, false);
		Buffer outputBuffer = Buffers.blockingQueueBuffer(new ArrayBlockingQueue<>(blob.getMinimumBufferCapacity(outputToken)), false, false);
		ImmutableMap<Token, Buffer> bufferMap = ImmutableMap.<Token, Buffer>builder()
				.put(inputToken, inputBuffer)
				.put(outputToken, outputBuffer)
				.build();
		blob.installBuffers(bufferMap);

		ImmutableList.Builder<PollingCoreThread> threads = ImmutableList.builder();
		for (int i = 0; i < blob.getCoreCount(); ++i) {
			PollingCoreThread thread = new PollingCoreThread(blob.getCoreCode(i), blob.toString()+"-"+i);
			threads.add(thread);
			thread.start();
		}

		return new BlobHostCompiledStream<>(blob, inputBuffer, outputBuffer, threads.build());
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

	private static final class BlobHostCompiledStream<I, O> implements CompiledStream<I, O> {
		private final Blob blob;
		private final Buffer inputBuffer, outputBuffer;
		private final ImmutableList<PollingCoreThread> threads;
		private BlobHostCompiledStream(Blob blob, Buffer inputBuffer, Buffer outputBuffer, ImmutableList<PollingCoreThread> threads) {
			this.blob = blob;
			this.inputBuffer = inputBuffer;
			this.outputBuffer = outputBuffer;
			this.threads = threads;
		}

		@Override
		public boolean offer(I input) {
			return inputBuffer.write(input);
		}

		@Override
		@SuppressWarnings("unchecked")
		public O poll() {
			return (O)outputBuffer.read();
		}

		@Override
		public void drain() {
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
	}

	private static final class PollingCoreThread extends Thread {
		private final Runnable coreCode;
		private volatile boolean running = true;
		private PollingCoreThread(Runnable target, String name) {
			super(name);
			this.coreCode = target;
		}
		@Override
		public void run() {
			while (running)
				coreCode.run();
		}
		public void requestStop() {
			running = false;
		}
	}
}
