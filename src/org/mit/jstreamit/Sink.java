package org.mit.jstreamit;

/**
 * A Sink pops data items and discards them.  Sinks are useful for terminating
 * stream graphs whose output will not be retrieved, preventing it from wasting
 * memory in queues.
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/2/2013
 */
public class Sink<T> extends Filter<T, Void> {
	public Sink() {
		super(1, 0);
	}

	@Override
	public void work() {
		pop();
	}

	@Override
	public Sink<T> copy() {
		return new Sink<>();
	}
}
