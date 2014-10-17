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
 * Programmers building a stream graph can either create instances of Splitjoin
 * for one-off pipelines, or create subclasses of Splitjoin that create and pass
 * SteamElement instances to the superclass constructor.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public class Splitjoin<I, O> implements OneToOneElement<I, O> {
	//We'd like this to be a Splitter<I, T>, but that would require introducing
	//T as a type variable in Splitjoin.
	private final Splitter splitter;
	private final Joiner joiner;
	private final List<OneToOneElement<?, ?>> elements;
	public <T, U> Splitjoin(Splitter<I, T> splitter, Joiner<U, O> joiner, OneToOneElement<? super T, ? extends U>... elements) {
		this(splitter, joiner, Arrays.asList(elements));
	}

	public <T, U> Splitjoin(Splitter<I, T> splitter, Joiner<U, O> joiner, List<? extends OneToOneElement<? super T, ? extends U>> elements) {
		int elems = elements.size();
		int splitOuts = splitter.supportedOutputs();
		int joinIns = joiner.supportedInputs();
		//If the splitter and joiner want different numbers of inputs and
		//outputs, and one of them isn't allowing any number, the combination is
		//invalid.
		if (splitOuts != joinIns && (splitOuts != Splitter.UNLIMITED && joinIns != Joiner.UNLIMITED))
			throw new IllegalArgumentException(String.format("Splitter produces %d outputs but joiner consumes %d inputs", splitOuts, joinIns));
		//TODO: these checks must be deferred until we're ready to go
//		if (splitOuts != Splitter.UNLIMITED && splitOuts != elems)
//			throw new IllegalArgumentException(String.format("Splitter expects %d outputs but %d elements provided", splitOuts, elems));
//		if (joinIns != Joiner.UNLIMITED && joinIns != elems)
//			throw new IllegalArgumentException(String.format("Joiner expects %d inputs but %d elements provided", joinIns, elems));
		this.splitter = splitter;
		this.joiner = joiner;
		this.elements = new ArrayList<>(elements.size());
		add(elements);
	}

	public final void add(OneToOneElement<?, ?> element) {
		if (element == null)
			throw new NullPointerException();
		if (element == this)
			throw new IllegalArgumentException("Adding splitjoin to itself");
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
		if (v.enterSplitjoin0(this)) {
			splitter.visit(v);
			for (OneToOneElement<?, ?> e : elements) {
				if (v.enterSplitjoinBranch(e)) {
					e.visit(v);
					v.exitSplitjoinBranch(e);
				}
			}
			joiner.visit(v);
			v.exitSplitjoin0(this);
		}
	}
}
