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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.util.Pair;
import java.util.Set;

/**
 * ParallelCompositeStreamVisitor composes multiple visitors into one visitor by
 * interleaving method calls to each visitor.  ParallelCompositeStreamVisitor resepects
 * return values from the enterFoo() methods by not forwarding calls after a
 * false return.
 *
 * ParallelCompositeStreamVisitor can be used directly by passing visitors to its
 * constructor, or by subclassing it and passing visitors to the superclass
 * constructor.
 * @see SerialCompositeStreamVisitor
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/8/2013
 */
public class ParallelCompositeStreamVisitor extends StreamVisitor {
	private final ImmutableSet<StreamVisitor> visitors;
	/**
	 * Contains a map of (element, call) pairs to the visitors that should be
	 * reenabled when we reach the given call on the given element.  We have to
	 * track the call so that we can distinguish false returns from
	 * enterSplitjoinBranch() and a following enterPipeline()/enterSplitjoin();
	 * in the former we must reenable in exitSplitjoinBranch(), while in the
	 * latter in exitPipeline()/exitSplitjoin(), but they would have the same
	 * element and so we might call exitSplitjoinBranch() even if we should skip
	 * it.
	 */
	private final SetMultimap<Pair<StreamElement<?, ?>, String>, StreamVisitor> disabled;
	public ParallelCompositeStreamVisitor(Set<StreamVisitor> visitors) {
		this.visitors = ImmutableSet.copyOf(visitors);
		this.disabled = HashMultimap.create(this.visitors.size(), this.visitors.size());
	}
	public ParallelCompositeStreamVisitor(StreamVisitor firstVisitor, StreamVisitor... moreVisitors) {
		this(ImmutableSet.copyOf(Lists.asList(firstVisitor, moreVisitors)));
	}

	private ImmutableSet<StreamVisitor> enabledVisitors() {
		return Sets.difference(visitors, ImmutableSet.copyOf(disabled.values())).immutableCopy();
	}
	private void disableUntil(StreamVisitor v, String call, StreamElement<?, ?> e) {
		disabled.put(new Pair<StreamElement<?, ?>, String>(e, call), v);
	}
	private void reenableAt(StreamElement<?, ?> e, String call) {
		disabled.removeAll(new Pair<StreamElement<?, ?>, String>(e, call));
	}

	@Override
	public final void beginVisit() {
		for (StreamVisitor v : enabledVisitors())
			v.beginVisit();
	}

	@Override
	public final void visitFilter(Filter<?, ?> filter) {
		for (StreamVisitor v : enabledVisitors())
			v.visitFilter(filter);
	}

	@Override
	public final boolean enterPipeline(Pipeline<?, ?> pipeline) {
		for (StreamVisitor v : enabledVisitors())
			if (!v.enterPipeline(pipeline))
				disableUntil(v, "exitPipeline", pipeline);
		return maybeContinue(pipeline, "exitPipeline");
	}

	@Override
	public final void exitPipeline(Pipeline<?, ?> pipeline) {
		for (StreamVisitor v : enabledVisitors())
			v.exitPipeline(pipeline);
		reenableAt(pipeline, "exitPipeline");
	}

	@Override
	public final boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		for (StreamVisitor v : enabledVisitors())
			if (!v.enterSplitjoin(splitjoin))
				disableUntil(v, "exitSplitjoin", splitjoin);
		return maybeContinue(splitjoin, "exitSplitjoin");
	}

	@Override
	public final void visitSplitter(Splitter<?, ?> splitter) {
		for (StreamVisitor v : enabledVisitors())
			v.visitSplitter(splitter);
	}

	@Override
	public final boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		for (StreamVisitor v : enabledVisitors())
			if (!v.enterSplitjoinBranch(element))
				disableUntil(v, "exitSplitjoinBranch", element);
		return maybeContinue(element, "exitSplitjoinBranch");
	}

	@Override
	public final void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		for (StreamVisitor v : enabledVisitors())
			v.exitSplitjoinBranch(element);
		reenableAt(element, "exitSplitjoinBranch");
	}

	@Override
	public final void visitJoiner(Joiner<?, ?> joiner) {
		for (StreamVisitor v : enabledVisitors())
			v.visitJoiner(joiner);
	}

	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		for (StreamVisitor v : enabledVisitors())
			v.exitSplitjoin(splitjoin);
		reenableAt(splitjoin, "exitSplitjoin");
	}

	@Override
	public final void endVisit() {
		for (StreamVisitor v : enabledVisitors())
			v.endVisit();
	}

	/**
	 * If we're about to enter an element and there are no enabled visitors, we
	 * can skip the element.  However, as this skips the exit call, we have to
	 * reenable visitors as though we'd received it.
	 * @return true iff we should enter the element
	 */
	private boolean maybeContinue(StreamElement<?, ?> element, String call) {
		if (!enabledVisitors().isEmpty())
			return true;
		reenableAt(element, call);
		return false;
	}

	@Override
	public String toString() {
		return "Composite: "+visitors;
	}
}
