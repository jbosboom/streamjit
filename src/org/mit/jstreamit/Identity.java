package org.mit.jstreamit;

/**
 * An Identity filter simply passes its input through to its output.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public final class Identity<T> extends Filter<T, T> {
	public Identity() {
		super(1, 1);
	}
	@Override
	public void work() {
		push(pop());
	}
}
