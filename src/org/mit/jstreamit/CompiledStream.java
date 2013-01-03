package org.mit.jstreamit;

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
	//public void drain();
}
