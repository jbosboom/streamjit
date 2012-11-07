package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class RoundrobinJoiner<T> extends Joiner<T, T> {
	private final int inputs;
	//TODO: we'd like to use UNLIMITED instead, but we can't write the work
	//function that way...
	public RoundrobinJoiner(int inputs) {
		this.inputs = inputs;
	}

	@Override
	public void work() {
		for (int i = 0; i < inputs; ++i)
			push(pop(i));
	}

	@Override
	public int inputs() {
		return inputs;
	}
}
