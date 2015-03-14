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
package edu.mit.streamjit.impl.interp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.BlobHostStreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.InputBufferFactory.ManualInputDelegate;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TODO: new comments now that we're running on a separate thread
 *
 * As its name suggests, this
 * compiler is intended for debugging purposes; it is unlikely to provide good
 * performance.
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/20/2012
 */
public class DebugStreamCompiler extends BlobHostStreamCompiler {
	public DebugStreamCompiler() {
		super(new DebugInterpreterBlobFactory());
	}
	@Override
	public String toString() {
		return "DebugStreamCompiler";
	}

	private static final class DebugInterpreterBlobFactory extends Interpreter.InterpreterBlobFactory {
		@Override
		public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState) {
			//TODO: find message constraints
			return new DebugInterpreter(workers, Collections.<MessageConstraint>emptyList(), config, initialState);
		}
		@Override
		public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
			Configuration superConfig = super.getDefaultConfiguration(workers);
			Configuration.Builder builder = Configuration.builder(superConfig);
			builder.removeParameter("channelFactory");
			List<ChannelFactory> universe = ImmutableList.<ChannelFactory>of(new DebugChannelFactory());
			builder.addParameter(new SwitchParameter<>("channelFactory", ChannelFactory.class, universe.get(0), universe));
			return builder.build();
		}
		private static final class DebugChannelFactory implements ChannelFactory {
			@Override
			public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
				return new DebugChannel<>();
			}
			@Override
			public boolean equals(Object other) {
				return other instanceof DebugChannelFactory;
			}
			@Override
			public int hashCode() {
				return 0;
			}
		}
	}

	private static final class DebugInterpreter extends Interpreter {
		private DebugInterpreter(Iterable<Worker<?, ?>> workersIter, Iterable<MessageConstraint> constraintsIter, Configuration config, DrainData initialState) {
			super(workersIter, constraintsIter, config, initialState);
		}

		@Override
		protected void afterFire(Worker<?, ?> worker) {
			checkRatesAfterFire(worker);
		}

		/**
		 * Checks the given worker (which has just fired)'s rate declarations
		 * against its actual behavior.
		 */
		private static <I, O> void checkRatesAfterFire(Worker<I, O> worker) {
			List<Channel<? extends I>> inputChannels = Workers.getInputChannels(worker);
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

			List<Channel<? super O>> outputChannels = Workers.getOutputChannels(worker);
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
		private boolean fullyDrained = true;
		/**
		 * Constructs a new UndrainedVisitor for a stream with the given input
		 * and output channels.
		 */
		UndrainedVisitor(Buffer streamInput) {
			if (streamInput.size() > 0)
				fullyDrained = false;
		}

		public boolean isFullyDrained() {
			return fullyDrained;
		}

		private void visitWorker(Worker<?, ?> worker) {
			//Every input channel except for the very first in the stream is an
			//output channel of some other worker, and we checked the first one
			//in the constructor, so we only need to check output channels here.
			for (Channel<?> c : Workers.getOutputChannels(worker))
				if (!c.isEmpty())
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
		@Override
		public void beginVisit() {
		}
		@Override
		public void endVisit() {
		}
	}

	public static void main(String[] args) {
		DebugStreamCompiler dsc = new DebugStreamCompiler();
		Input.ManualInput<Integer> input = Input.createManualInput();
		Output.ManualOutput<Integer> output = Output.createManualOutput();
		CompiledStream cs = dsc.compile(new Identity<Integer>(), input, output);
		Object o = null;
		for (int i = 0; i < 10;) {
			if (input.offer(i))
				++i;
			if ((o = output.poll()) != null)
				System.out.println(o);
		}
		input.drain();
		for (int i = 0; i < 11; ++i)
			System.out.println(output.poll());
	}
}
