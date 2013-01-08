package org.mit.jstreamit;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/3/2013
 */
public abstract class AbstractCompiledStream<I, O> implements CompiledStream<I, O> {
	private final Channel<? super I> inputChannel;
	private final Channel<? extends O> outputChannel;
	private volatile boolean draining = false;
	private volatile boolean fullyDrained = false;
	private final CountDownLatch awaitDrainingLatch = new CountDownLatch(1);

	/**
	 * Creates a new AbstractCompiledStream, using the given channels for input
	 * and output.  (Hint: if you're using a separate thread, use synchronized
	 * channels.)
	 * @param inputChannel
	 * @param outputChannel
	 */
	public AbstractCompiledStream(Channel<? super I> inputChannel, Channel<? extends O> outputChannel) {
		if (inputChannel == null || outputChannel == null)
			throw new NullPointerException();
		this.inputChannel = inputChannel;
		this.outputChannel = outputChannel;
	}

	@Override
	public final boolean offer(I input) {
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
	public final O poll() {
		try {
			return outputChannel.pop();
		} catch (NoSuchElementException ex) {
			//Channel is empty.
			return null;
		}
	}

	public final void drain() {
		draining = true;
		doDrain();
	}

	public final boolean awaitDraining() throws InterruptedException {
		awaitDrainingLatch.await();
		return fullyDrained;
	}

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

	protected final void finishedDraining(boolean fullyDrained) {
		this.fullyDrained = fullyDrained;
		awaitDrainingLatch.countDown();
	}
}
