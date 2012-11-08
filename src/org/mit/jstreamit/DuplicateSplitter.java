package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class DuplicateSplitter<T> extends Splitter<T, T>{
	public DuplicateSplitter() {
	}

	@Override
	public void work() {
		T item = pop();
		for (int i = 0; i < outputs(); ++i)
			push(i, item);
	}

	@Override
	public int supportedOutputs() {
		return Splitter.UNLIMITED;
	}
}
