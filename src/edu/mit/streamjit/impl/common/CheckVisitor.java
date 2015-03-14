/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.UnbalancedSplitjoinException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.util.Fraction;
import edu.mit.streamjit.util.Pair;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks a stream graph for validity.
 *
 * CheckVisitor leaves the stream graph disconnected.  It is safe to use
 * CheckVisitor with a connected graph, but it will need to be re-connected
 * afterward, and data in channels may be lost to the garbage collector if the
 * channels are not strongly referenced elsewhere.
 *
 * TODO: CheckVisitor does not correctly reason about 0 rates, which can be used
 * to "inject" data into a graph with a splitjoin; even legal uses will be
 * reported as errors.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/8/2013
 */
public final class CheckVisitor extends SerialCompositeStreamVisitor {
	public CheckVisitor() {
		super(new NoDuplicatesVisitor(),
				new SourceSinkVisitor(),
				new SplitterJoinerMatchBranches(),
				//These visitors depend on connectivity.
				new ConnectWorkersVisitor(),
				new SplitjoinBranchRateBalanceVisitor(),
				new PeekPopCheckVisitor(),
				new DisconnectWorkersVisitor());
	}

	private static final class NoDuplicatesVisitor extends StackVisitor {
		/**
		 * Maps each StreamElement in the graph to the trace at its first
		 * appearance.  (If we see an element again, we now have two traces to
		 * report in the exception.)
		 */
		private final Map<StreamElement<?, ?>, ImmutableList<GraphTraceElement>> appearance = new HashMap<>();
		@Override
		protected void visitFilter0(Filter<?, ?> filter) {
			checkElement(filter);
		}

		@Override
		protected boolean enterPipeline0(Pipeline<?, ?> pipeline) {
			checkElement(pipeline);
			return true;
		}

		@Override
		protected void exitPipeline0(Pipeline<?, ?> pipeline) {
		}

		@Override
		protected boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
			checkElement(splitjoin);
			return true;
		}

		@Override
		protected void visitSplitter0(Splitter<?, ?> splitter) {
			checkElement(splitter);
		}

		@Override
		protected boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element) {
			//Do nothing -- we'll check the element when we visit it separately.
			return true;
		}

		@Override
		protected void exitSplitjoinBranch0(OneToOneElement<?, ?> element) {
		}

		@Override
		protected void visitJoiner0(Joiner<?, ?> joiner) {
			checkElement(joiner);
		}

		@Override
		protected void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
		}

		private void checkElement(StreamElement<?, ?> element) {
			ImmutableList<GraphTraceElement> oldTrace = appearance.get(element);
			ImmutableList<GraphTraceElement> newTrace = getTrace();
			if (oldTrace != null)
				throw new ElementRepeatedException(element, oldTrace, newTrace);
			appearance.put(element, newTrace);
		}

		private static final class ElementRepeatedException extends IllegalStreamGraphException {
			private static final long serialVersionUID = 1L;
			private final String traceStrings;
			@SafeVarargs
			private ElementRepeatedException(StreamElement<?, ?> element, ImmutableList<GraphTraceElement> firstTrace, ImmutableList<GraphTraceElement> secondTrace, ImmutableList<GraphTraceElement>... moreTraces) {
				super("Element appears more than once in stream graph", element);
				this.traceStrings = com.google.common.base.Joiner.on("\n\n").join(
						Iterables.transform(Lists.asList(firstTrace, secondTrace, moreTraces), StackVisitor::asTrace));
			}

			@Override
			public String toString() {
				return super.toString()+"\n"+traceStrings;
			}
		}
	}

	private static final class SplitterJoinerMatchBranches extends StackVisitor {
		private final Multiset<Splitjoin<?, ?>> branchCount = HashMultiset.create();
		private final Map<Splitjoin<?, ?>, Splitter<?, ?>> splitters = new HashMap<>();
		private final Map<Splitjoin<?, ?>, Joiner<?, ?>> joiners = new HashMap<>();
		@Override
		protected void visitFilter0(Filter<?, ?> filter) {
		}
		@Override
		protected boolean enterPipeline0(Pipeline<?, ?> pipeline) {
			return true;
		}
		@Override
		protected void exitPipeline0(Pipeline<?, ?> pipeline) {
		}
		@Override
		protected boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
			return true;
		}
		@Override
		protected void visitSplitter0(Splitter<?, ?> splitter) {
			splitters.put((Splitjoin<?, ?>)getTrace().get(1).getElement(), splitter);
		}
		@Override
		protected boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element) {
			branchCount.add((Splitjoin<?, ?>)getTrace().get(1).getElement());
			return true;
		}
		@Override
		protected void exitSplitjoinBranch0(OneToOneElement<?, ?> element) {
		}
		@Override
		protected void visitJoiner0(Joiner<?, ?> joiner) {
			joiners.put((Splitjoin<?, ?>)getTrace().get(1).getElement(), joiner);
		}
		@Override
		protected void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
			int branches = branchCount.count(splitjoin);
			Splitter<?, ?> splitter = splitters.get(splitjoin);
			int supportedOutputs = splitter.supportedOutputs();
			if (supportedOutputs != Splitter.UNLIMITED && supportedOutputs != branches)
				throw new IllegalStreamGraphException(
						String.format("%s supports %d outputs, but %s has %d branches%n%s%n",
						splitter, supportedOutputs, splitjoin, branches,
						asTrace(getTrace())),
						splitter, splitjoin);
			Joiner<?, ?> joiner = joiners.get(splitjoin);
			int supportedInputs = joiner.supportedInputs();
			if (supportedInputs != Joiner.UNLIMITED && supportedInputs != branches)
				throw new IllegalStreamGraphException(
						String.format("%s supports %d inputs, but %s has %d branches%n%s%n",
						joiner, supportedInputs, splitjoin, branches,
						asTrace(getTrace())),
						joiner, splitjoin);
		}
	}

	private static final class SplitjoinBranchRateBalanceVisitor extends StackVisitor {
		private Range<Fraction> currentRate;
		private final Deque<SplitjoinContext> splitjoins = new ArrayDeque<>();
		private static final class SplitjoinContext {
			private Range<Fraction> rateAtEntry;
			private Splitter<?, ?> splitter;
			//Has the splitter's rate factored in; gets the joiner's rate once
			//we reach the joiner.
			private final List<Range<Fraction>> branchRates = new ArrayList<>();
		}
		@Override
		public void beginVisit() {
			super.beginVisit();
			currentRate = Range.closed(Fraction.ONE, Fraction.ONE);
		}
		@Override
		protected void visitFilter0(Filter<?, ?> filter) {
			Range<Fraction> range = toRange(filter.getPopRates().get(0), filter.getPushRates().get(0));
			currentRate = multiply(currentRate, range);
		}
		@Override
		protected boolean enterPipeline0(Pipeline<?, ?> pipeline) {
			return true;
		}
		@Override
		protected void exitPipeline0(Pipeline<?, ?> pipeline) {
		}
		@Override
		protected boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
			splitjoins.push(new SplitjoinContext());
			splitjoins.peek().rateAtEntry = currentRate;
			return true;
		}
		@Override
		protected void visitSplitter0(Splitter<?, ?> splitter) {
			splitjoins.peek().splitter = splitter;
		}
		@Override
		protected boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element) {
			SplitjoinContext ctx = splitjoins.peek();
			int branchIndex = ctx.branchRates.size();
			currentRate = toRange(ctx.splitter.getPopRates().get(0), ctx.splitter.getPushRates().get(branchIndex));
			return true;
		}
		@Override
		protected void exitSplitjoinBranch0(OneToOneElement<?, ?> element) {
			splitjoins.peek().branchRates.add(currentRate);
		}
		@Override
		protected void visitJoiner0(Joiner<?, ?> joiner) {
			List<Range<Fraction>> branchRates = splitjoins.peek().branchRates;
			for (int i = 0; i < branchRates.size(); ++i) {
				Range<Fraction> joinerRate = toRange(joiner.getPopRates().get(i), joiner.getPushRates().get(0));
				branchRates.set(i, multiply(branchRates.get(i), joinerRate));
			}
		}
		@Override
		protected void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
			SplitjoinContext ctx = splitjoins.pop();
			Range<Fraction> splitjoinRate = ctx.branchRates.get(0);
			for (int i = 1; i < ctx.branchRates.size(); ++i) {
				Range<Fraction> branchRate = ctx.branchRates.get(i);
				if (!splitjoinRate.isConnected(branchRate) ||
						(splitjoinRate = splitjoinRate.intersection(branchRate)).isEmpty())
					throw new UnbalancedSplitjoinException(splitjoin, ctx.branchRates);
			}
			currentRate = multiply(ctx.rateAtEntry, splitjoinRate);
		}
		@Override
		public void endVisit() {
			super.endVisit();
			assert splitjoins.isEmpty();
		}

		private Range<Fraction> toRange(Rate pop, Rate push) {
			//TODO: figure out what to do here
			if (pop.isDynamic())
				return Range.greaterThan(Fraction.ZERO);
			int pushMin = push.min() == Rate.DYNAMIC ? 0 : push.min();
			Fraction lowerBound = new Fraction(pushMin, pop.max());
			if (push.max() == Rate.DYNAMIC)
				return Range.greaterThan(lowerBound);
			Fraction upperBound = new Fraction(push.max(), pop.min());
			return Range.closed(lowerBound, upperBound);
		}
		private Range<Fraction> multiply(Range<Fraction> first, Range<Fraction> second) {
			Fraction lowerBound = first.lowerEndpoint().mul(second.lowerEndpoint());
			if (!first.hasUpperBound() || !second.hasUpperBound())
				return Range.greaterThan(lowerBound);
			Fraction upperBound = first.upperEndpoint().mul(second.upperEndpoint());
			return Range.closed(lowerBound, upperBound);
		}
	}

	/**
	 * Checks that all but the first worker are not sources (pop 0), and that
	 * all but the last worker are not sinks (push 0).
	 *
	 * TODO: This could be more efficient by not storing everything until the
	 * end.
	 */
	private static final class SourceSinkVisitor extends StackVisitor {
		private final ImmutableList.Builder<Pair<Worker<?, ?>, ImmutableList<GraphTraceElement>>> traces = ImmutableList.builder();
		private SourceSinkVisitor() {
		}
		@Override
		protected void visitFilter0(Filter<?, ?> filter) {
			visitWorker(filter);
		}
		@Override
		protected boolean enterPipeline0(Pipeline<?, ?> pipeline) {
			return true;
		}
		@Override
		protected void exitPipeline0(Pipeline<?, ?> pipeline) {
		}
		@Override
		protected boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
			return true;
		}
		@Override
		protected void visitSplitter0(Splitter<?, ?> splitter) {
		}
		@Override
		protected boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element) {
			return true;
		}
		@Override
		protected void exitSplitjoinBranch0(OneToOneElement<?, ?> element) {
		}
		@Override
		protected void visitJoiner0(Joiner<?, ?> joiner) {
		}
		@Override
		protected void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
		}
		private void visitWorker(Worker<?, ?> worker) {
			traces.add(Pair.<Worker<?, ?>, ImmutableList<GraphTraceElement>>make(worker, getTrace()));
		}
		@Override
		public void endVisit() {
			ImmutableList<Pair<Worker<?, ?>, ImmutableList<GraphTraceElement>>> list = traces.build();
			for (Pair<Worker<?, ?>, ImmutableList<GraphTraceElement>> p : list.subList(1, list.size()))
				for (Rate r : p.first.getPopRates())
					if (r.max() == 0)
						//TODO: trace
						throw new IllegalStreamGraphException("source in middle of graph", p.first);
			for (Pair<Worker<?, ?>, ImmutableList<GraphTraceElement>> p : list.subList(0, list.size()-1))
				for (Rate r : p.first.getPushRates())
					if (r.max() == 0)
						//TODO: trace
						throw new IllegalStreamGraphException("sink in middle of graph", p.first);
			super.endVisit();
		}
	}

	/**
	 * Checks that every worker has the same number of peek and pop rates.  For
	 * joiners that support a fixed number of inputs, also checks that number
	 * matches the number of peek and pop rates.
	 *
	 * This visitor requires the graph be connected because some splitters and
	 * joiners provide rates based on how many inputs and outputs they have.
	 */
	private static final class PeekPopCheckVisitor extends StackVisitor {
		@Override
		protected void visitFilter0(Filter<?, ?> filter) {
			visitWorker(filter);
		}
		@Override
		protected boolean enterPipeline0(Pipeline<?, ?> pipeline) {
			return true;
		}
		@Override
		protected void exitPipeline0(Pipeline<?, ?> pipeline) {
		}
		@Override
		protected boolean enterSplitjoin0(Splitjoin<?, ?> splitjoin) {
			return true;
		}
		@Override
		protected void visitSplitter0(Splitter<?, ?> splitter) {
			visitWorker(splitter);
		}
		@Override
		protected boolean enterSplitjoinBranch0(OneToOneElement<?, ?> element) {
			return true;
		}
		@Override
		protected void exitSplitjoinBranch0(OneToOneElement<?, ?> element) {
		}
		@Override
		protected void visitJoiner0(Joiner<?, ?> joiner) {
			visitWorker(joiner);
			int supported = joiner.supportedInputs();
			int peeks = joiner.getPeekRates().size(), pops = joiner.getPopRates().size();
			if (supported != Joiner.UNLIMITED) {
				if (peeks != supported)
					throw new IllegalStreamGraphException(String.format("supports %d inputs, but has %d peek rates", supported, peeks), joiner);
				if (pops != supported)
					throw new IllegalStreamGraphException(String.format("supports %d inputs, but has %d pop rates", supported, pops), joiner);
			}
		}
		@Override
		protected void exitSplitjoin0(Splitjoin<?, ?> splitjoin) {
		}
		private void visitWorker(Worker<?, ?> worker) {
			if (worker.getPeekRates().size() != worker.getPopRates().size())
				//TODO: trace
				throw new IllegalStreamGraphException("different number of peek and pop rates", worker);
		}
	}
}
