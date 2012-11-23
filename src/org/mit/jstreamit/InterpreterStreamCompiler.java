package org.mit.jstreamit;

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
		private PrimitiveWorker<?, ?> cur;
		private Channel head = new Channel(), tail = new Channel();
		@Override
		public void visitFilter(Filter filter) {
			if (cur == null) {
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
		public boolean enterPipeline(Pipeline pipeline) {
			//Nothing to do but visit the pipeline elements.
			return true;
		}

		@Override
		public void exitPipeline(Pipeline<?, ?> pipeline) {
			//Nothing to do here.
		}

		@Override
		public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void visitJoiner(Joiner<?, ?> joiner) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void visitSplitter(Splitter<?, ?> splitter) {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	}
}
