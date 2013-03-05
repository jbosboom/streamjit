package edu.mit.streamjit;

import edu.mit.streamjit.api.Filter;

/**
 * A BlackHole pops data items and discards them. BlackHoles are useful for
 * terminating stream graphs whose output will not be retrieved, preventing it
 * from wasting memory in queues.
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/2/2013
 */
public class BlackHole<T> extends Filter<T, Void> {
	public BlackHole() {
		super(1, 0);
	}

	@Override
	public void work() {
		pop();
	}
}
