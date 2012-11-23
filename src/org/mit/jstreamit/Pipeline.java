package org.mit.jstreamit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TODO: pipelines are not compile-time type-safe!
 *
 * Programmers building a stream graph can either create instances of Pipeline
 * for one-off pipelines, or create subclasses of Pipeline that create and pass
 * OneToOneElement instances to the superclass constructor.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class Pipeline<I, O> implements OneToOneElement<I, O> {
	private final List<OneToOneElement<?, ?>> elements;
	public Pipeline(OneToOneElement<?, ?>... elements) {
		this(Arrays.asList(elements));
	}
	public Pipeline(List<OneToOneElement<?, ?>> elements) {
		this.elements = new ArrayList<>(elements);
	}

	public final void add(OneToOneElement<?, ?> first, OneToOneElement<?, ?>... more) {
		elements.add(first);
		elements.addAll(Arrays.asList(more));
	}

	@Override
	public final Pipeline<I, O> copy() {
		List<OneToOneElement<?, ?>> elementsCopy = new ArrayList<>(elements.size());
		for (OneToOneElement<?, ?> element : elements) {
			OneToOneElement<?, ?> elementCopy = element.copy();
			//To detect misbehaving copy() implementations...
			assert element != elementCopy : element;
			assert element.getClass() == elementCopy.getClass() : element + ", " + elementCopy;
			elementsCopy.add(element.copy());
		}
		return new Pipeline<>(elementsCopy);
	}

	@Override
	public final void visit(StreamVisitor v) {
		if (v.enterPipeline(this)) {
			for (OneToOneElement<?, ?> e : elements)
				e.visit(v);
			v.exitPipeline(this);
		}
	}
}
