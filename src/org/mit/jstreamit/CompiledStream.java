package org.mit.jstreamit;

/**
 * The interface to a compiled stream.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public interface CompiledStream<I, O> {
	public void put(I input);
	public void take(I output);
	//public void drain();
}
