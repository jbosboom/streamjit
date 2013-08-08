package edu.mit.streamjit.impl.common;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
public final class CheckVisitor extends CompositeStreamVisitor {
	public CheckVisitor() {
		super(new NoDuplicatesVisitor());
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

	public static void main(String[] args) {
		Identity<Integer> id = new Identity<>();
		Pipeline<Integer, Integer> p = new Pipeline<>(id, id);
		p.visit(new CheckVisitor());
	}
}
