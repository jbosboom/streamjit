package org.mit.jstreamit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A StreamCompiler that only interprets the stream graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public class InterpreterStreamCompiler implements StreamCompiler {
	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {
		StreamElement<I, O> copy = stream.copy();
//		PrimitiveWorker<?, ?> head = firstChild(copy);
		return null;
	}

	/**
	 * Visits the stream graph, connecting the primitive workers with channels
	 * and keeping references to the first input and last output channels.
	 *
	 * This class uses lots of raw types to avoid having to recapture the
	 * unbounded wildcards all the time.
	 */
	private static class ConnectPrimitiveWorkersVisitor extends StreamVisitor {
		private Channel head = new Channel(), tail = new Channel();
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

		@Override
		public void visitFilter(Filter filter) {
			if (cur == null) { //First worker encountered.
				//No predecessor to go with this input channel.
				filter.getInputChannels().add(head);
			} else {
				Channel c = new Channel();
				cur.addSuccessor(filter, c);
				filter.addPredecessor(cur, c);
			}
			cur = filter;
		}

		@Override
		public boolean enterPipeline(Pipeline<?, ?> pipeline) {
			//Nothing to do but visit the pipeline elements.
			return true;
		}

		@Override
		public void exitPipeline(Pipeline<?, ?> pipeline) {
			//Nothing to do here.
		}

		@Override
		public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
			//Nothing to do but visit the splijoin elements.  (We push the new
			//SplitjoinContext in visitSplitter().)
			return true;
		}

		@Override
		public void visitSplitter(Splitter splitter) {
			if (cur == null) { //First worker encountered.
				//No predecessor to go with this input channel.
				splitter.getInputChannels().add(head);
			} else {
				Channel c = new Channel();
				cur.addSuccessor(splitter, c);
				splitter.addPredecessor(cur, c);
			}
			cur = splitter;

			SplitjoinContext ctx = new SplitjoinContext();
			ctx.splitter = splitter;
			stack.addFirst(ctx);
		}

		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			//Reset cur to the remembered splitter.
			cur = stack.peekFirst().splitter;
			//Visit subelements.
			return true;
		}

		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
			//Remember cur as a branch end.
			stack.peekFirst().branchEnds.add(cur);
		}

		@Override
		public void visitJoiner(Joiner joiner) {
			//Note that a joiner cannot be the first worker encountered because
			//joiners only occur in splitjoins and the splitter will be visited
			//first.
			for (PrimitiveWorker<?, ?> w : stack.peekFirst().branchEnds) {
				Channel c = new Channel();
				w.addSuccessor(joiner, c);
				joiner.addPredecessor(w, c);
			}

			stack.removeFirst();
			cur = joiner;
		}

		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
			//Nothing to do here.  (We pop the SplitjoinContext in
			//visitJoiner().)
		}
	}
}
