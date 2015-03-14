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
package edu.mit.streamjit.impl.common;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.StreamVisitor;
import java.util.Set;

/**
 * ParallelCompositeStreamVisitor composes multiple visitors into one visitor by
 * sequentially performing full visitations.
 *
 * @see ParallelCompositeStreamVisitor
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/9/2013
 */
public class SerialCompositeStreamVisitor extends StreamVisitor {
	private final ImmutableSet<StreamVisitor> visitors;
	/**
	 * We record the first element, terminate the visitation, then start new
	 * visitations from the visitors in endVisit().  This is a total hack, but
	 * I don't see how else to compose visitors sequentially.
	 */
	private StreamElement<?, ?> firstElement = null;
	public SerialCompositeStreamVisitor(Set<StreamVisitor> visitors) {
		this.visitors = ImmutableSet.copyOf(visitors);
	}
	public SerialCompositeStreamVisitor(StreamVisitor firstVisitor, StreamVisitor... moreVisitors) {
		this(ImmutableSet.copyOf(Lists.asList(firstVisitor, moreVisitors)));
	}

	@Override
	public final void beginVisit() {
	}
	@Override
	public final void visitFilter(Filter<?, ?> filter) {
		firstElement = filter;
	}
	@Override
	public final boolean enterPipeline(Pipeline<?, ?> pipeline) {
		firstElement = pipeline;
		return false;
	}
	@Override
	public final void exitPipeline(Pipeline<?, ?> pipeline) {
		throw new AssertionError();
	}
	@Override
	public final boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		firstElement = splitjoin;
		return false;
	}
	@Override
	public final void visitSplitter(Splitter<?, ?> splitter) {
		throw new AssertionError();
	}
	@Override
	public final boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		throw new AssertionError();
	}
	@Override
	public final void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		throw new AssertionError();
	}
	@Override
	public final void visitJoiner(Joiner<?, ?> joiner) {
		throw new AssertionError();
	}
	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		throw new AssertionError();
	}
	@Override
	public final void endVisit() {
		//You can't visit nothing, so we should always have something.
		assert firstElement != null;
		for (StreamVisitor v : visitors)
			firstElement.visit(v);
	}
}
