package org.mit.jstreamit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TODO: pipelines are not compile-time type-safe!
 *
 * Programmers building a stream graph can either create instances of Pipeline
 * for one-off pipelines, or create subclasses of Pipeline that create and pass
 * SteamElement instances to the superclass constructor.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class Pipeline<I, O> implements StreamElement<I, O> {
	private final List<StreamElement<?, ?>> elements;
	public Pipeline(StreamElement<?, ?>... elements) {
		this(Arrays.asList(elements));
	}
	public Pipeline(List<StreamElement<?, ?>> elements) {
		this.elements = new ArrayList<>(elements);
	}

	public final void add(StreamElement<?, ?> first, StreamElement<?, ?>... more) {
		elements.add(first);
		elements.addAll(Arrays.asList(more));
	}
}
