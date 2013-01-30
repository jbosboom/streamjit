package org.mit.jstreamit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Visits the stream graph, connecting the primitive workers with channels
 * and keeping references to the first and last filters.  Subclasses override
 * the makeChannel() method to provide Channels to connect filters together.
 * This visitor does not add input or output channels to the first or last
 * filters respectively.
 *
 * This class uses lots of raw types to avoid having to recapture the
 * unbounded wildcards all the time.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/23/2013 (originally internal to DebugStreamCompiler)
 */
abstract class ConnectPrimitiveWorkersVisitor extends StreamVisitor {
	/**
	 * The first worker in the stream graph.
	 */
	private PrimitiveWorker<?, ?> source;
	/**
	 * During the visitation, the last worker encountered.  After the visitation
	 * is complete, the last worker in the stream graph.
	 */
	private PrimitiveWorker<?, ?> cur;
	private Deque<SplitjoinContext> stack = new ArrayDeque<>();

	/**
	 * Used for remembering information about splitjoins while traversing:
	 * so we can reset cur to splitter when we enter a branch, and so we
	 * record cur at the end of a branch to connect it to the joiner after
	 * all branches are processed.
	 */
	private static class SplitjoinContext {
		private Splitter<?, ?> splitter;
		private List<PrimitiveWorker<?, ?>> branchEnds = new ArrayList<>();
	}

	protected ConnectPrimitiveWorkersVisitor() {}

	/**
	 * Creates a Channel object to be used to connect the given two workers
	 * together.
	 *
	 * TODO: generic bounds are too strict -- the filters don't have to exactly
	 * agree on type, but merely be compatible
	 * @param <E> the type of element in the Channel
	 * @param upstream the upstream worker
	 * @param downstream the downstream worker
	 * @return a Channel that will be used to connect the upstream and
	 * downstream workers
	 */
	protected abstract <E> Channel<E> makeChannel(PrimitiveWorker<?, E> upstream, PrimitiveWorker<E, ?> downstream);

	/**
	 * After the visitation is complete, returns the first worker in the stream
	 * graph.
	 * @returnthe first worker in the stream graph
	 */
	public final PrimitiveWorker<?, ?> getSource() {
		return source;
	}

	/**
	 * After the visitation is complete, returns the last worker in the stream
	 * graph.
	 * @return last worker in the stream graph
	 */
	public final PrimitiveWorker<?, ?> getSink() {
		return cur;
	}

	@Override
	public final void visitFilter(Filter filter) {
		visitWorker(filter);
	}

	@Override
	public final boolean enterPipeline(Pipeline<?, ?> pipeline) {
		//Nothing to do but visit the pipeline elements.
		return true;
	}

	@Override
	public final void exitPipeline(Pipeline<?, ?> pipeline) {
		//Nothing to do here.
	}

	@Override
	public final boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		//Nothing to do but visit the splijoin elements.  (We push the new
		//SplitjoinContext in visitSplitter().)
		return true;
	}

	@Override
	public final void visitSplitter(Splitter splitter) {
		visitWorker(splitter);
		SplitjoinContext ctx = new SplitjoinContext();
		ctx.splitter = splitter;
		stack.push(ctx);
	}

	@Override
	public final boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		//Reset cur to the remembered splitter.
		cur = stack.peek().splitter;
		//Visit subelements.
		return true;
	}

	@Override
	public final void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		//Remember cur as a branch end.
		stack.peek().branchEnds.add(cur);
	}

	@Override
	public final void visitJoiner(Joiner joiner) {
		//Note that a joiner cannot be the first worker encountered because
		//joiners only occur in splitjoins and the splitter will be visited
		//first.
		for (PrimitiveWorker<?, ?> w : stack.peekFirst().branchEnds) {
			Channel c = makeChannel(w, joiner);
			w.addSuccessor(joiner, c);
			joiner.addPredecessor(w, c);
		}

		stack.pop();
		cur = joiner;
	}

	@Override
	public final void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		//Nothing to do here.  (We pop the SplitjoinContext in
		//visitJoiner().)
	}

	private void visitWorker(PrimitiveWorker worker) {
		if (cur == null) { //First worker encountered.
			source = worker;
		} else {
			//TODO: move checks to a CheckVisitor, include checks for stream
			//graphs with no workers
			//cur isn't the last worker.
			for (Rate rate : cur.getPushRates())
				if (rate.max() == 0)
					throw new IllegalStreamGraphException("Sink isn't last worker", (StreamElement)cur);
			//worker isn't the first worker.
			for (Rate rate : cur.getPopRates())
				if (rate.max() == 0)
					throw new IllegalStreamGraphException("Source isn't first worker", (StreamElement)worker);

			Channel c = makeChannel(cur, worker);
			cur.addSuccessor(worker, c);
			worker.addPredecessor(cur, c);
		}
		cur = worker;
	}
}
