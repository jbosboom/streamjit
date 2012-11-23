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
		stream = stream.copy();
		ConnectPrimitiveWorkersVisitor cpwv = new ConnectPrimitiveWorkersVisitor();
		stream.visit(cpwv);
		Channel head = cpwv.head, tail = cpwv.tail;
		PrimitiveWorker<?, ?> source = cpwv.source, sink = cpwv.cur;
		//We don't know the iteration is over until it's over, so hook up the
		//tail channel here.
		sink.getOutputChannels().add(tail);

		return new InterpretedCompiledStream<>(head, tail, source, sink);
	}

	/**
	 * Note: not yet thread-safe!
	 * @param <I>
	 * @param <O>
	 */
	private static class InterpretedCompiledStream<I, O> implements CompiledStream<I, O> {
		private final Channel head, tail;
		private final PrimitiveWorker<?, ?> source, sink;
		InterpretedCompiledStream(Channel head, Channel tail, PrimitiveWorker<?, ?> source, PrimitiveWorker<?, ?> sink) {
			this.head = head;
			this.tail = tail;
			this.source = source;
			this.sink = sink;
		}

		@Override
		public void put(I input) {
			head.push(input);
			pull();
		}

		@Override
		public O take() {
			return (O)tail.pop();
		}

		/**
		 * Fires the sink as many times as possible (firing upstream filters as
		 * required).
		 */
		private void pull() {
			//Deliberate empty while-loop-body.
			while (fireOnceIfPossible(sink));
		}

		/**
		 * Fires upstream filters just enough to allow worker to fire, or
		 * returns false if this is impossible.
		 * @param worker the worker to fire
		 * @return true if the worker fired, false if it didn't
		 */
		private boolean fireOnceIfPossible(PrimitiveWorker<?, ?> worker) {
			//Use an explicit stack to avoid overflow.
			Deque<PrimitiveWorker<?, ?>> stack = new ArrayDeque<>();
			stack.push(worker);
			while (!stack.isEmpty()) {
				PrimitiveWorker<?, ?> current = stack.element();
				int channel = findUnsatisfiedChannel(current);
				if (channel == -1) {
					current.work();
					stack.pop();
				} else {
					if (current == source)
						return false;
					stack.push(current.getPredecessors().get(channel));
				}
			}
			return true;
		}

		/**
		 * Searches the given worker's input channels for one that requires more
		 * elements before the worker can fire, returning the index of the found
		 * channel or -1 if the worker can fire.
		 */
		private <I, O> int findUnsatisfiedChannel(PrimitiveWorker<I, O> worker) {
			List<Channel<? extends I>> channels = worker.getInputChannels();
			List<Rate> peekRates = worker.getPeekRates();
			List<Rate> popRates = worker.getPopRates();
			for (int i = 0; i < channels.size(); ++i) {
				Rate peek = peekRates.get(i), pop = popRates.get(i);
				if (peek.max() == Rate.DYNAMIC || pop.max() == Rate.DYNAMIC)
					throw new UnsupportedOperationException("Unbounded input rates not yet supported");
				int required = Math.max(peek.max(), pop.max());
				if (channels.get(i).size() < required)
					return i;
			}
			return -1;
		}
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
		private PrimitiveWorker<?, ?> cur, source;
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
				source = filter;
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
				source = splitter;
				splitter.getInputChannels().add(head);
			} else {
				Channel c = new Channel();
				cur.addSuccessor(splitter, c);
				splitter.addPredecessor(cur, c);
			}
			cur = splitter;

			SplitjoinContext ctx = new SplitjoinContext();
			ctx.splitter = splitter;
			stack.push(ctx);
		}

		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			//Reset cur to the remembered splitter.
			cur = stack.peek().splitter;
			//Visit subelements.
			return true;
		}

		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
			//Remember cur as a branch end.
			stack.peek().branchEnds.add(cur);
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

			stack.pop();
			cur = joiner;
		}

		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
			//Nothing to do here.  (We pop the SplitjoinContext in
			//visitJoiner().)
		}
	}
}
