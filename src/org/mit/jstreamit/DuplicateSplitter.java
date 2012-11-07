package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class DuplicateSplitter<T> extends Splitter<T, T>{
	private final int outputs;
	//TODO: we'd like to use UNLIMITED instead, but we can't write the work
	//function that way...
	public DuplicateSplitter(int outputs) {
		this.outputs = outputs;
	}

	@Override
	public void work() {
		T item = pop();
		for (int i = 0; i < outputs(); ++i)
			push(i, item);
	}

	@Override
	public int outputs() {
		return outputs;
	}
}
