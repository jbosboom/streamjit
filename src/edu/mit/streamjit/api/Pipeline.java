/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TODO: pipelines are not compile-time type-safe!
 *
 * Programmers building a stream graph can either create instances of Pipeline
 * for one-off pipelines, or create subclasses of Pipeline that create and pass
 * OneToOneElement instances to the superclass constructor.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public class Pipeline<I, O> implements OneToOneElement<I, O> {
	private final List<OneToOneElement<?, ?>> elements;
	public Pipeline(OneToOneElement<?, ?>... elements) {
		this(Arrays.asList(elements));
	}

	public Pipeline(List<? extends OneToOneElement<?, ?>> elements) {
		this.elements = new ArrayList<>(elements.size());
		add(elements);
	}

	public final void add(OneToOneElement<?, ?> element) {
		if (element == null)
			throw new NullPointerException();
		if (element == this)
			throw new IllegalArgumentException("Adding pipeline to itself");
		elements.add(element);
	}

	public final void add(OneToOneElement<?, ?> first, OneToOneElement<?, ?>... more) {
		add(first);
		add(Arrays.asList(more));
	}

	public final void add(List<? extends OneToOneElement<?, ?>> elements) {
		for (OneToOneElement<?, ?> element : elements)
			add(element);
	}

	@Override
	public final void visit(StreamVisitor v) {
		if (v.enterPipeline0(this)) {
			for (OneToOneElement<?, ?> e : elements)
				e.visit(v);
			v.exitPipeline0(this);
		}
	}
}
