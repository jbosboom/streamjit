package edu.mit.streamjit.impl.common;

import com.google.common.base.Function;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamElement;
import java.util.HashMap;
import java.util.Map;

/**
 * Checks a stream graph for validity.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/8/2013
 */
public final class CheckVisitor extends ParallelCompositeStreamVisitor {
	public CheckVisitor() {
		super(new NoDuplicatesVisitor(),
				new SplitterJoinerMatchBranches());
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
						Iterables.transform(Lists.asList(firstTrace, secondTrace, moreTraces),
						new Function<ImmutableList<GraphTraceElement>, String>() {
							@Override
							public String apply(ImmutableList<GraphTraceElement> input) {
								return asTrace(input);
							}
						}));
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
}
