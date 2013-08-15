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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
