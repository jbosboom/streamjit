package edu.mit.streamjit;

import java.util.concurrent.TimeUnit;

/**
 * The interface to a compiled stream.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public interface CompiledStream<I, O> {
	/**
	 * Submits the given element to the compiled stream if it is possible to do
	 * so immediately. Returns true if the element was submitted or false
	 * otherwise. Implementations might return false due to being out of buffer
	 * space (when elements are being submitted faster than they can be
	 * processed) or if the stream is draining or closed (due to another thread
	 * invoking drain()).
	 *
	 * @param input the element being submitted
	 * @return true iff the element was submitted
	 * @throws NullPointerException if input == null
	 */
	public boolean offer(I input);

	/**
	 * Retrieves an element from the compiled stream if it is possible to do so
	 * immediately. Returns the retrieved element or null if no element was
	 * retrieved. Implementations might return null if no output is currently
	 * available or if the stream is closed and all output has already been
	 * retrieved.
	 *
	 * @return an element of stream output, or null
	 */
	public O poll();

	/**
	 * Initiate draining this stream.  After this method returns, no elements
	 * can be added to the stream with offer(); all calls to offer() will fail
	 * by returning false.
	 *
	 * This method does not wait for the stream to drain; to block, use
	 * awaitDraining().
	 */
	void drain();

	/**
	 * Wait for this stream to finish draining.  If this stream has already
	 * finished draining, returns immediately.
	 *
	 * Note that calling this method will not cause the stream to drain if it
	 * has not already begun; if the intent is to drain and wait, call drain()
	 * before calling this method.
	 * @return true if the stream was fully drained, or false if elements were
	 * left in buffers
	 * @throws InterruptedException if this thread is interrupted while waiting
	 */
	boolean awaitDraining() throws InterruptedException;

	/**
	 * Wait up to a given duration for this stream to finish draining.  If this
	 * stream has already finished draining, returns immediately.
	 *
	 * Note that calling this method will not cause the stream to drain if it
	 * has not already begun; if the intent is to drain and wait, call drain()
	 * before calling this method.
	 * @param timeout the maximum time to wait
	 * @param unit the unit of the timeout argument
	 * @return true if the stream was fully drained, or false if elements were
	 * left in buffers
	 * @throws InterruptedException if this thread is interrupted while waiting
	 */
	boolean awaitDraining(long timeout, TimeUnit unit) throws InterruptedException;
}
