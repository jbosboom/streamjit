package org.mit.jstreamit;

import java.util.List;

/**
 * TODO: pipelines are not compile-time type-safe!
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public final class Pipeline<I, O> implements StreamElement<I, O> {
	private final StreamElement[] elements;
	public Pipeline(StreamElement... elements) {
		if (elements.length == 0)
			throw new IllegalArgumentException();
		this.elements = elements;
	}
	public Pipeline(List<StreamElement<?, ?>> elements) {
		this.elements = elements.toArray(new StreamElement[0]);
	}
}
