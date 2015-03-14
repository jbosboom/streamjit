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
package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Datasets;

/**
 * TODO: messaging is currently broken (something with Portal/constraint
 * registration?)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/24/2013
 */
public class DownstreamMessageTest {
//	public static void main(String[] args) throws InterruptedException {
//		Benchmark benchmark = new DownstreamMessageTestBenchmark();
//		StreamCompiler dsc = new DebugStreamCompiler();
//		CompiledStream stream = dsc.compile(benchmark.instantiate(), benchmark.inputs().get(0).input(), Output.toPrintStream(System.out));
//		stream.awaitDrained();
//	}
//
//	@ServiceProvider(Benchmark.class)
//	public static final class DownstreamMessageTestBenchmark extends SuppliedBenchmark {
//		public DownstreamMessageTestBenchmark() {
//			super("DownstreamMessageTest", new Supplier<OneToOneElement<Object, Object>>() {
//				@Override
//				@SuppressWarnings("unchecked")
//				public OneToOneElement<Object, Object> get() {
//					Portal<MessageInterface> portal = new Portal<>(MessageInterface.class);
//					Pipeline<Integer, Integer> stream = new Pipeline<>();
//					stream.add(new MessageSender(portal));
//					MessageRecipient mr = new MessageRecipient();
//					portal.addRecipient(mr);
//					stream.add(mr);
//					return (OneToOneElement)stream;
//				}
//			}, Dataset.builder(Datasets.allIntsInRange(0, 10)).build());
//		}
//	}

	private static interface MessageInterface {
		public void handler(int value);
	}

	private static class MessageSender extends Filter<Integer, Integer> {
		private final Portal<MessageInterface> portal;
		MessageSender(Portal<MessageInterface> portal) {
			super(1, 1);
			this.portal = portal;
		}
		@Override
		public void work() {
			int x = pop();
			portal.getHandle(this, 0).handler(-x);
			push(x);
		}
	}

	private static class MessageRecipient extends StatefulFilter<Integer, Integer> implements MessageInterface {
		private int lastMessage = Integer.MIN_VALUE;
		MessageRecipient() {
			super(1, 2, 0);
		}

		@Override
		public void work() {
			int x = pop();
			push(x);
			push(lastMessage);
		}

		@Override
		public void handler(int value) {
			lastMessage = value;
		}
	}
}
