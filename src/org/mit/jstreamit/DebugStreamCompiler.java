package org.mit.jstreamit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A StreamCompiler that interprets the stream graph on the thread that calls
 * CompiledStream.put(). This compiler performs extra checks to verify filters
 * conform to their rate declarations. The CompiledStream returned from the
 * compile() method synchronizes offer() and poll() such that only up to one
 * element is being offered or polled at once. As its name suggests, this
 * compiler is intended for debugging purposes; it is unlikely to provide good
 * performance.
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/20/2012
 */
public class DebugStreamCompiler implements StreamCompiler {
	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {
		//TODO: I'd like to make a copy here so the user can't mess with our
		//stuff, but that will screw up portal registration.
//		stream = stream.copy();
		ConnectPrimitiveWorkersVisitor cpwv = new ConnectPrimitiveWorkersVisitor() {
			@Override
			protected <E> Channel<E> makeChannel(PrimitiveWorker<?, E> upstream, PrimitiveWorker<E, ?> downstream) {
				return new DebugChannel<>();
			}
		};
		stream.visit(cpwv);
		PrimitiveWorker<I, ?> source = (PrimitiveWorker<I, ?>)cpwv.getSource();
		DebugChannel<I> head = new DebugChannel<>();
		source.getInputChannels().add(head);
		PrimitiveWorker<?, O> sink = (PrimitiveWorker<?, O>)cpwv.getSink();
		DebugChannel<O> tail = new DebugChannel<>();
		sink.getOutputChannels().add(tail);
		return new DebugCompiledStream<>(head, tail, source, sink);
	}

	/**
	 * This CompiledStream synchronizes offer() and poll(), so it can use
	 * unsynchronized Channels.
	 *
	 * TODO: should we use bounded buffers here?
	 * @param <I> the type of input data elements
	 * @param <O> the type of output data elements
	 */
	private static class DebugCompiledStream<I, O> extends AbstractCompiledStream<I, O> {
		private final PrimitiveWorker<?, ?> source, sink;
		DebugCompiledStream(Channel<? super I> head, Channel<? extends O> tail, PrimitiveWorker<?, ?> source, PrimitiveWorker<?, ?> sink) {
			super(head, tail);
			this.source = source;
			this.sink = sink;
		}

		@Override
		public synchronized boolean offer(I input) {
			boolean ret = super.offer(input);
			pull();
			return ret;
		}

		@Override
		public synchronized O poll() {
			if (sink.getPushRates().get(0).max() == 0)
				throw new IllegalStateException("Can't take() from a stream ending in a sink");
			return super.poll();
		}

		@Override
		protected synchronized void doDrain() {
			//Most implementations of doDrain() hand off to another thread to
			//avoid blocking in drain(), but we only have one thread.
			pull();
			//We need to see if any elements were left undrained.
			UndrainedVisitor v = new UndrainedVisitor(source.getInputChannels().get(0), sink.getOutputChannels().get(0));
			finishedDraining(v.isFullyDrained());
		}

		/**
		 * Fires the sink as many times as possible (firing upstream filters as
		 * required).
		 */
		private void pull() {
			//We should be in the offer() method that owns our lock.
			assert Thread.holdsLock(this) : "pull() without lock?";
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
					checkedFire(current);
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

		/**
		 * Fires the given worker, checking the items consumed and produced
		 * against its rate declarations.
		 */
		private <I, O> void checkedFire(PrimitiveWorker<I, O> worker) {
			worker.doWork();
			List<Channel<? extends I>> inputChannels = worker.getInputChannels();
			List<Rate> popRates = worker.getPopRates();
			List<Rate> peekRates = worker.getPeekRates();
			for (int i = 0; i < inputChannels.size(); ++i) {
				Rate peek = peekRates.get(i), pop = popRates.get(i);
				//All channels we create are DebugChannels, so this is safe.
				DebugChannel<? extends I> channel = (DebugChannel<? extends I>)inputChannels.get(i);
				int peekIndex = channel.getMaxPeekIndex();
				if (peek.min() != Rate.DYNAMIC && peekIndex+1 < peek.min() ||
						peek.max() != Rate.DYNAMIC && peekIndex+1 > peek.max())
					throw new AssertionError(String.format("%s: Peek rate %s but peeked at index %d on channel %d", worker, peek, peekIndex, i));
				int popCount = channel.getPopCount();
				if (pop.min() != Rate.DYNAMIC && popCount < pop.min() ||
						pop.max() != Rate.DYNAMIC && popCount > pop.max())
					throw new AssertionError(String.format("%s: Pop rate %s but popped %d elements from channel %d", worker, peek, popCount, i));
				channel.resetStatistics();
			}

			List<Channel<? super O>> outputChannels = worker.getOutputChannels();
			List<Rate> pushRates = worker.getPushRates();
			for (int i = 0; i < outputChannels.size(); ++i) {
				Rate push = pushRates.get(i);
				//All channels we create are DebugChannels, so this is safe.
				DebugChannel<? super O> channel = (DebugChannel<? super O>)outputChannels.get(i);
				int pushCount = channel.getPushCount();
				if (push.min() != Rate.DYNAMIC && pushCount < push.min() ||
						push.max() != Rate.DYNAMIC && pushCount > push.max())
					throw new AssertionError(String.format("%s: Push rate %s but pushed %d elements onto channel %d", worker, push, pushCount, i));
				channel.resetStatistics();
			}
		}
	}

	/**
	 * Checks if a stream fully drained or not.
	 *
	 * TODO: check for pending messages?
	 */
	private static class UndrainedVisitor extends StreamVisitor {
		private final Channel<?> streamOutput;
		private boolean fullyDrained = true;
		/**
		 * Constructs a new UndrainedVisitor for a stream with the given input
		 * and output channels.
		 */
		UndrainedVisitor(Channel<?> streamInput, Channel<?> streamOutput) {
			this.streamOutput = streamOutput;
			if (!streamInput.isEmpty())
				fullyDrained = false;
		}

		public boolean isFullyDrained() {
			return fullyDrained;
		}

		private void visitWorker(PrimitiveWorker<?, ?> worker) {
			//Every input channel except for the very first in the stream is an
			//output channel of some other worker, and we checked the first one
			//in the constructor, so we only need to check output channels here.
			for (Channel<?> c : worker.getOutputChannels())
				//Ignore the stream's final output, as it doesn't count as
				//"undrained" even if it hasn't been picked up yet.
				if (c != streamOutput && !c.isEmpty())
					fullyDrained = false;
		}
		@Override
		public void visitFilter(Filter<?, ?> filter) {
			visitWorker(filter);
		}
		@Override
		public boolean enterPipeline(Pipeline<?, ?> pipeline) {
			//Enter the pipeline only if we haven't found undrained data yet.
			return fullyDrained;
		}
		@Override
		public void exitPipeline(Pipeline<?, ?> pipeline) {
		}
		@Override
		public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
			//Enter the splitjoin only if we haven't found undrained data yet.
			return fullyDrained;
		}
		@Override
		public void visitSplitter(Splitter<?, ?> splitter) {
			visitWorker(splitter);
		}
		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			//Enter the branch only if we haven't found undrained data yet.
			return fullyDrained;
		}
		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		}
		@Override
		public void visitJoiner(Joiner<?, ?> joiner) {
			visitWorker(joiner);
		}
		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		}
	}
}
