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

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.StreamVisitor;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A StackVisitor is a StreamVisitor that keeps a stack trace of the visitation
 * in progress. The top of the stack is the element currently being visited (in
 * enter and exit methods, the element being entered or exited); the second
 * element is that element's parent, and so on.
 * <p/>
 * StackVisitor needs to run code around the subclass implementation, so it
 * provides final implementations of the StreamVisitor methods with
 * corresponding abstract methods with a 0 appended (e.g., visitFilter0). These
 * methods have the same contract as the corresponding StreamVisitor methods.
 * beginVisit() and endVisit() are not final and may be overridden, though the
 * subclass should call the superimplementation.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/8/2013
 */
public abstract class StackVisitor extends StreamVisitor {
	private final Deque<GraphTraceElement> stack = new ArrayDeque<>();
	protected StackVisitor() {}

	protected abstract void visitFilter0(Filter<?, ?> filter);
	protected abstract boolean enterPipeline0(Pipeline<?, ?> pipeline);
	protected abstract void exitPipeline0(Pipeline<?, ?> pipeline);
	protected abstract boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin);
	protected abstract void visitSplitter0(Splitter<?, ?> splitter);
	protected abstract boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element);
	protected abstract void exitSplitjoinBranch0(OneToOneElement<?, ?> element);
	protected abstract void visitJoiner0(Joiner<?, ?> joiner);
	protected abstract void exitSplitjoin0(Splitjoin<?, ?> splitjoin);

	protected final ImmutableList<GraphTraceElement> getTrace() {
		return ImmutableList.copyOf(stack);
	}

	@Override
	public void beginVisit() {
	}

	@Override
	public void endVisit() {
		assert stack.isEmpty();
	}

	//<editor-fold defaultstate="collapsed" desc="StreamVisitor method implementations">
	@Override
	public final void visitFilter(Filter<?, ?> filter) {
		maybeIncrementPipelineElement();
		stack.push(new LeafGraphTraceElement(filter));
		visitFilter0(filter);
		stack.pop();
	}

	@Override
	public final boolean enterPipeline(Pipeline<?, ?> pipeline) {
		maybeIncrementPipelineElement();
		stack.push(new LeafGraphTraceElement(pipeline));
		boolean enter = enterPipeline0(pipeline);
		stack.pop();
		if (enter) {
			stack.push(new InternalGraphTraceElement(pipeline, -1));
			return true;
		} else
			return false;
	}

	@Override
	public final void exitPipeline(Pipeline<?, ?> pipeline) {
		exitPipeline0(pipeline);
		GraphTraceElement pop = stack.pop();
		assert pop.getElement() == pipeline;
	}

	@Override
	public final boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		maybeIncrementPipelineElement();
		stack.push(new LeafGraphTraceElement(splitjoin));
		boolean enter = enterSplitjoin0(splitjoin);
		if (enter)
			//We leave the splitjoin on the stack; the splitter will fix it up.
			return true;
		else {
			stack.pop();
			return false;
		}
	}

	@Override
	public final void visitSplitter(Splitter<?, ?> splitter) {
		GraphTraceElement pop = stack.pop();
		assert pop.getElement() instanceof Splitjoin;
		stack.push(new InternalGraphTraceElement(pop.getElement(),
				InternalGraphTraceElement.SPLITTER_SUBELEM));

		stack.push(new LeafGraphTraceElement(splitter));
		visitSplitter0(splitter);
		stack.pop();
	}

	@Override
	public final boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		InternalGraphTraceElement pop = (InternalGraphTraceElement)stack.pop();
		assert pop.getElement() instanceof Splitjoin;
		stack.push(new InternalGraphTraceElement(pop.getElement(), pop.subelement + 1));

		stack.push(new LeafGraphTraceElement(element));
		boolean enter = enterSplitjoinBranch0(element);
		//Always pop the stack -- whatever we're about to enter will push its
		//own trace element.
		stack.pop();
		return enter;
	}

	@Override
	public final void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		exitSplitjoinBranch0(element);
		//Don't pop the splitjoin element -- we'll need it in
		//enterSplitjoinBranch or visitJoiner.
	}

	@Override
	public final void visitJoiner(Joiner<?, ?> joiner) {
		InternalGraphTraceElement pop = (InternalGraphTraceElement)stack.pop();
		assert pop.getElement() instanceof Splitjoin;
		stack.push(new InternalGraphTraceElement(pop.getElement(),
				InternalGraphTraceElement.JOINER_SUBELEM));

		stack.push(new LeafGraphTraceElement(joiner));
		visitJoiner0(joiner);
		stack.pop();
	}

	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		exitSplitjoin0(splitjoin);
		GraphTraceElement pop = stack.pop();
		assert pop.getElement() == splitjoin;
	}

	/**
	 * There's no enterPipelineElement() method, so whenever we visit a filter
	 * or enter a pipeline or splitjoin, we need to check if we should increment
	 * a containing pipeline's subelement.
	 */
	private void maybeIncrementPipelineElement() {
		GraphTraceElement top = stack.peek();
		if (top == null) return;
		if (top.getElement() instanceof Pipeline) {
			stack.pop();
			stack.push(new InternalGraphTraceElement(top.getElement(),
					((InternalGraphTraceElement)top).subelement + 1));
		}
	}
	//</editor-fold>

	/**
	 * Represents a location in the stream graph, analogous to
	 * StackTraceElement.
	 * <p/>
	 * TODO: getSubelement()? Would require support from Pipeline and Splitjoin,
	 * possibly through a ContainerElement interface.
	 * <p/>
	 * TODO: Should this be exposed in the API so we can include it directly
	 * (rather than as text) in IllegalStreamGraphException and subclasses?
	 */
	public interface GraphTraceElement {
		public StreamElement<?, ?> getElement();
		@Override
		public String toString();
	}

	/**
	 * Returns a String representation of the given list of GraphTraceElements,
	 * assuming the first element is the deepest in the graph.
	 * @param elements a graph trace
	 * @return a String representation of the trace
	 */
	public static String asTrace(Iterable<GraphTraceElement> elements) {
		//TODO Java 8: move this into GraphTraceElement
		return com.google.common.base.Joiner.on("\nin ").join(elements);
	}

	private static final class InternalGraphTraceElement implements GraphTraceElement {
		/**
		 * SPLITTER_SUBELEM being -1 is important for correctness in
		 * enterSplitjoinBranch.
		 */
		private static final int SPLITTER_SUBELEM = -1, JOINER_SUBELEM = -2;
		private final StreamElement<?, ?> element;
		private final int subelement;
		private InternalGraphTraceElement(StreamElement<?, ?> element, int subelement) {
			this.element = element;
			this.subelement = subelement;
		}
		@Override
		public StreamElement<?, ?> getElement() {
			return element;
		}
		@Override
		public String toString() {
			String subelem;
			if (subelement == SPLITTER_SUBELEM)
				subelem = "splitter";
			else if (subelement == JOINER_SUBELEM)
				subelem = "joiner";
			else if (element instanceof Pipeline)
				subelem = "element "+subelement;
			else if (element instanceof Splitjoin)
				subelem = "branch "+subelement;
			else
				throw new AssertionError();
			return String.format("%s, %s", element, subelem);
		}
	}

	private static final class LeafGraphTraceElement implements GraphTraceElement {
		private final StreamElement<?, ?> element;
		private LeafGraphTraceElement(StreamElement<?, ?> element) {
			assert element != null;
			this.element = element;
		}
		@Override
		public StreamElement<?, ?> getElement() {
			return element;
		}
		@Override
		public String toString() {
			return element.toString();
		}
	}
}
