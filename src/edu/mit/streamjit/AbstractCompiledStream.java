package edu.mit.streamjit;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A skeletal implementation of CompiledStream that forwards offer() and poll()
 * to push() and pop() operations on Channel instances.
 *
 * This class provides a properly-synchronized implementation of drain() and
 * related methods using volatile variables and a CountDownLatch.  offer() and
 * poll() are not synchronized; if the implementation requires interacting with
 * other threads, synchronized Channel implementations should be used.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/3/2013
 */
public abstract class AbstractCompiledStream<I, O> implements CompiledStream<I, O> {
	/**
	 * The channel being used for input: offer() turns into push().
	 */
	private final Channel<? super I> inputChannel;
	/**
	 * The channel being used for output: poll() turns into pop().
	 */
	private final Channel<? extends O> outputChannel;
	/**
	 * Whether the stream is in the process of draining (or already drained);
	 * specifically, whether calls to offer() should be forwarded to the channel
	 * or not.
	 */
	private volatile boolean draining = false;
	/**
	 * Once the awaitDrainingLatch is cleared, indicates whether the stream was
	 * fully drained or if there were data items stuck in buffers.
	 */
	private volatile boolean fullyDrained = false;
	/**
	 * The latch that threads blocked in awaitDraining() block on.
	 */
	private final CountDownLatch awaitDrainingLatch = new CountDownLatch(1);

	/**
	 * Creates a new AbstractCompiledStream, using the given channels for input
	 * and output.  If the implementation does anything with threads, these
	 * channels should be synchronized.  If the stream has a source and/or a
	 * sink, consider using an EmptyChannel for input and/or output.
	 * @param inputChannel the channel to use for input
	 * @param outputChannel the channel to use for output
	 */
	public AbstractCompiledStream(Channel<? super I> inputChannel, Channel<? extends O> outputChannel) {
		if (inputChannel == null || outputChannel == null)
			throw new NullPointerException();
		this.inputChannel = inputChannel;
		this.outputChannel = outputChannel;
	}

	@Override
	public boolean offer(I input) {
		if (draining)
			return false;

		try {
			inputChannel.push(input);
		} catch (IllegalStateException ex) {
			//Capacity-bounded channel is full.
			return false;
		}
		return true;
	}

	@Override
	public O poll() {
		try {
			return outputChannel.pop();
		} catch (NoSuchElementException ex) {
			//Channel is empty.
			return null;
		}
	}

	@Override
	public final void drain() {
		draining = true;
		doDrain();
	}

	@Override
	public final boolean awaitDraining() throws InterruptedException {
		awaitDrainingLatch.await();
		return fullyDrained;
	}

	@Override
	public final boolean awaitDraining(long timeout, TimeUnit unit) throws InterruptedException {
		awaitDrainingLatch.await(timeout, unit);
		return fullyDrained;
	}

	/**
	 * Called when a client calls drain() on this stream, to be implemented by
	 * subclasses to begin draining and resource reclamation.  Note that this
	 * method is called on the thread that called drain(), which should not
	 * block; if interactions with other threads are required, the
	 * implementation should set a flag or otherwise communicate the drain
	 * request to another thread.  finishedDraining() should be called when
	 * draining is finished.
	 */
	protected abstract void doDrain();

	/**
	 * Called by an implementation when draining is complete, to release clients
	 * blocked in awaitDraining().  This method should only be called once.
	 * @param fullyDrained true if the stream was fully drained, false if data
	 * items remained in buffers
	 */
	protected final void finishedDraining(boolean fullyDrained) {
		//TODO: enforce that the method is called once?
		this.fullyDrained = fullyDrained;
		awaitDrainingLatch.countDown();
	}
}
