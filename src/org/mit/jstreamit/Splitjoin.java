package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class Splitjoin<I, O> implements StreamElement<I, O> {
	//We'd like this to be a Splitter<I, T>, but that would require introducing
	//T as a type variable in Splitjoin.
	private final Splitter splitter;
	private final Joiner joiner;
	private final StreamElement[] elements;
	public <T, U> Splitjoin(Splitter<I, T> splitter, Joiner<U, O> joiner, StreamElement<? super T, ? extends U>... elements) {
		int elems = elements.length;
		if (elems == 0)
			throw new IllegalArgumentException("Splitjoin without elements");
		int splitOuts = splitter.outputs();
		if (splitOuts != Integer.MAX_VALUE && splitOuts != elems)
			throw new IllegalArgumentException(String.format("Splitter expects %d outputs but %d elements provided", splitOuts, elems));
		int joinIns = joiner.inputs();
		if (joinIns != Integer.MAX_VALUE && joinIns != elems)
			throw new IllegalArgumentException(String.format("Joiner expects %d inputs but %d elements provided", joinIns, elems));
		this.splitter = splitter;
		this.joiner = joiner;
		this.elements = elements;
	}
}
