package edu.mit.streamjit.api;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The interface to a compiled stream.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public interface CompiledStream {
	/**
	 * Returns true if the stream has finished draining. Once this method
	 * returns true, it will always return true in the future. Once a thread
	 * observes this method to return true, all data has been written to the
	 * output (if using ManualOutput, the thread can retrieve all output data
	 * items by calling ManualOutput.poll() until poll() returns null).
	 * <p/>
	 * Note that busy-waiting on isDrained() without calling poll() may lead to
	 * deadlock; either alternate polling and checking for draining, or use two
	 * separate threads.
	 * @return true if the stream has finished draining
	 */
	public boolean isDrained();

	public void awaitDrained() throws InterruptedException;
	public void awaitDrained(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;
}
