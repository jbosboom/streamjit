package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class RoundrobinJoiner<T> extends Joiner<T, T> {
	public RoundrobinJoiner() {
	}

	@Override
	public void work() {
		for (int i = 0; i < inputs(); ++i)
			push(pop(i));
	}

	@Override
	public int supportedInputs() {
		return Joiner.UNLIMITED;
	}
}
