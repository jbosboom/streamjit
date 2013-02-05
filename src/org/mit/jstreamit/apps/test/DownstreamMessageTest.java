package org.mit.jstreamit.apps.test;

import org.mit.jstreamit.CompiledStream;
import org.mit.jstreamit.DebugStreamCompiler;
import org.mit.jstreamit.Filter;
import org.mit.jstreamit.OneToOneElement;
import org.mit.jstreamit.Pipeline;
import org.mit.jstreamit.Portal;
import org.mit.jstreamit.StatefulFilter;
import org.mit.jstreamit.StreamCompiler;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/24/2013
 */
public class DownstreamMessageTest {
	public static void main(String[] args) throws InterruptedException {
		Portal<MessageInterface> portal = new Portal<>(MessageInterface.class);
		Pipeline<Integer, Integer> stream = new Pipeline<>();
		stream.add(new MessageSender(portal));
		MessageRecipient mr = new MessageRecipient();
		portal.addRecipient(mr);
		stream.add(mr);

		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream<Integer, Integer> compiledStream = sc.compile(stream);
		for (int i = 0; i < 10; ++i)
			compiledStream.offer(i);
		compiledStream.drain();
		compiledStream.awaitDraining();
		Integer x = null;
		while ((x = compiledStream.poll()) != null)
			System.out.println(x);
	}

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
