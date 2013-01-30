package org.mit.jstreamit;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/8/2012
 */
public final class Portal<I> {
	private final Class<I> klass;
	private final List<I> recipients = new ArrayList<>();
	public Portal(Class<I> klass) {
		if (!klass.isInterface())
			throw new IllegalArgumentException(klass+" is not an interface type");
		//TODO: are these checks too strict?  The interpreter can check these
		//dynamically and the compiler can tell exactly which methods are called,
		//so we could be more lenient here.
		for (Method m : klass.getMethods()) {
			if (m.getDeclaringClass().equals(Object.class))
				continue;
			if (!m.getReturnType().equals(void.class))
				throw new IllegalArgumentException("Method "+m.toGenericString()+" in "+klass+" returns non-void");
			//TODO: do we need m.getGenericExceptionTypes() to handle "throws E"?
			if (m.getExceptionTypes().length > 0)
				throw new IllegalArgumentException("Method "+m.toGenericString()+" in "+klass+" may throw");
		}
		this.klass = klass;
	}

	/**
	 * Registers a message recipient with this portal.  The recipient must
	 * implement the interface specified by this portal.
	 *
	 * This method should only be called while building a stream graph.  After
	 * the stream graph is compiled, calling this method will result in
	 * undefined behavior.
	 * @param recipient the message recipient to add
	 * @throws NullPointerException if recipient is null
	 * @throws IllegalArgumentException if recipient does not implement this
	 * portal's interface
	 */
	public void addRecipient(I recipient) {
		//TODO: public <T extends PrimitiveWorker & I> void addRecipient(T recipient)
		if (recipient == null)
			throw new NullPointerException();
		//I'm pretty sure this can only happen via unchecked casts or
		//incompatible class file changes, but we should check anyway.
		if (!klass.isInstance(recipient))
			throw new IllegalArgumentException("Recipient "+recipient+" not instance of "+klass);
		//Messaging a non-worker doesn't make sense -- SDEP isn't defined.
		if (!(recipient instanceof PrimitiveWorker))
			throw new IllegalArgumentException("Recipient "+recipient+" not instance of Filter, Splitter or Joiner");
		recipients.add(recipient);
	}

	/**
	 * Gets a handle from this portal which can be used to send messages to the
	 * registered recipients.  This method should only be called from the work()
	 * function of a filter, splitter or joiner.
	 *
	 * The sender argument should always be the this reference of the filter,
	 * splitter or joiner from which getHandle() is being called; other values
	 * will result in strange behavior. Unfortunately, the Java language does
	 * not allow enforcing this requirement.
	 *
	 * TODO briefly explain latency
	 *
	 * The returned handle appears to be an I instance, but is actually a magic
	 * object that translates calls of I methods to messages. That is, calling a
	 * method on the returned handle with some arguments results in that method
	 * being invoked with those arguments on each of the recipients after the
	 * specified latency. Only methods declared in I or a superinterface of I
	 * may be invoked through the handle; Object methods may not be invoked. The
	 * argument objects must not be modified until after all recipients have
	 * received and processed their messages. Handles should not be stored in
	 * local variables or fields.
	 *
	 * Implementation note: this is a JIT hook method.
	 *
	 * TODO: PrimitiveWorker is package-private and we're
	 * leaking it into the public API here.  Alternatives:
	 * --create three overloads of getHandle: Filter, Splitter and Joiner
	 * --just make PrimitiveWorker public (probably renamed to Worker and with
	 *   all the Channel/predecessor/successor stuff still package-private)
	 *
	 * TODO: latency ranges?
	 * @param sender the message sender
	 * @param latency the message latency
	 * @return an I whose calls generate messages
	 */
	public I getHandle(PrimitiveWorker<?, ?> sender, int latency) {
		if (sender == null)
			throw new NullPointerException();
		//When running in the JIT compiler, this is a JIT hook instead.
		@SuppressWarnings("unchecked")
		I handle = (I)Proxy.newProxyInstance(klass.getClassLoader(), new Class<?>[]{klass}, new Handle<>(sender, recipients, latency));
		return handle;
	}

	/* package-private */ static class Message implements Comparable<Message> {
		public final Method method;
		public final Object[] args;
		public int executionsUntilDelivery;
		Message(Method method, Object[] args, int executionsUntilDelivery) {
			this.method = method;
			this.args = args;
			this.executionsUntilDelivery = executionsUntilDelivery;
		}
		@Override
		public int compareTo(Message o) {
			return Integer.compare(executionsUntilDelivery, o.executionsUntilDelivery);
		}
	}

	/**
	 * The back-end of the dynamic proxy created in getHandle() (strictly
	 * speaking, this isn't the actual handle object, but HandleHandler seemed
	 * like a dumb name).
	 */
	private static class Handle<I> implements InvocationHandler {
		private final PrimitiveWorker<?, ?> sender;
		private final List<I> recipients;
		private final int latency;
		private Handle(PrimitiveWorker<?, ?> sender, List<I> recipients, int latency) {
			this.sender = sender;
			this.recipients = recipients;
			this.latency = latency;
		}
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			//We check in the Portal constructor that all non-Object methods are
			//valid to call through, so check we aren't calling an Object method.
			if (method.getDeclaringClass().equals(Object.class))
				throw new IllegalStreamGraphException("Call to Object method "+method+" through portal", sender);

			//We probably don't have access to the message interface, but we
			//need to call its methods anyway.  This might fail under a security
			//manager, or if the interface is somehow security sensitive to the
			//Java platform(?).
			method.setAccessible(true);

			for (I recipientI : recipients) {
				//Compute the time-to-delivery.
				int executionsUntilDelivery = Integer.MIN_VALUE;
				//The cast is safe because we check in addRecipient() that only
				//PrimitiveWorkers can be added.
				PrimitiveWorker<?, ?> recipient = (PrimitiveWorker<?, ?>)recipientI;
				switch (sender.compareStreamPosition(recipient)) {
					case -1: //sender is upstream of recipient; message travels downstream
						if (latency < 0)
							throw new UnsupportedOperationException("TODO: downstream messages with negative latency");

						for (int m = 0; ; ++m)
							if (sdep(sender, recipient, m) >= latency) {
								executionsUntilDelivery = m;
								break;
							}
						break;

					case 1: //sender is downstream of recipient; message travels upstream
						if (latency < 0)
							throw new IllegalStreamGraphException(
									String.format("Sending a message upstream from %s to %s via portal %s with negative latency %d", sender, recipient, this, latency),
									(StreamElement<?, ?>)sender, (StreamElement<?, ?>)recipient);

						throw new UnsupportedOperationException("TODO: upstream messages with positive latency");
						//break;

					case 0: //sender and recipient are incomparable
						throw new IllegalStreamGraphException(
								String.format("Sending a message between incomparable elements %s and %s via portal %s", sender, recipient, this),
								(StreamElement<?, ?>)sender, (StreamElement<?, ?>)recipient);
					default:
						throw new AssertionError("Can't happen!");
				}

				//Queue up the message at the recipient.
				Message message = new Message(method, args, executionsUntilDelivery);
				recipient.sendMessage(message);
			}

			//Methods on the portal interface return void.
			return null;
		}
	}

	/**
	 * Compute SDEP(h, t, executions); that is, the minimum number of times h
	 * must execute for t to execute executions times. If executions is 0, the
	 * result is 0. If h == t, the result is executions. If there is no path
	 * from h to t in the stream graph, t does not depend on h, so the result is
	 * 0.
	 * @param h the upstream worker
	 * @param t the downstream worker
	 * @param executions the number of executions of t to ensure
	 * @return the number of executions of h required
	 */
	private static int sdep(PrimitiveWorker<?, ?> h, PrimitiveWorker<?, ?> t, int executions) {
		if (h == null || t == null)
			throw new NullPointerException();
		if (executions < 0)
			throw new IllegalArgumentException(Integer.toString(executions));
		if (executions == 0)
			return 0;
		if (h == t)
			return executions;

		//If t is an immediate successor of h, we can compute by looking at
		//their data rates.
		int hIndex = h.getSuccessors().indexOf(t);
		if (hIndex != -1) {
			Rate hPushRate = h.getPushRates().get(hIndex);
			int tIndex = t.getPredecessors().indexOf(h);
			assert tIndex != -1;
			Rate tPeekRate = t.getPeekRates().get(tIndex), tPopRate = t.getPopRates().get(tIndex);
			if (hPushRate.isDynamic())
				throw new IllegalStreamGraphException("Sending message over dynamic input rates", t);
			if (tPeekRate.isDynamic() || tPopRate.isDynamic())
				throw new IllegalStreamGraphException("Sending message over dynamic input rates", t);

			int itemsRequired = requiredToExecute(tPeekRate.max(), tPopRate.max(), executions);
			int hFires = 0, itemsProduced = 0;
			while (itemsProduced < itemsRequired) {
				++hFires;
				itemsProduced += hPushRate.min();
			}
			return hFires;
		}

		//Otherwise, check all of h's successors that are predecessors of t.
		int result = 0;
		Set<PrimitiveWorker<?, ?>> tPredecessors = t.getAllPredecessors();
		for (PrimitiveWorker<?, ?> s : h.getSuccessors()) {
			if (!tPredecessors.contains(s))
				continue;
			int x = sdep(s, t, executions);
			result = Math.max(result, sdep(h, s, x));
		}

		return result;
	}

	private static int requiredToExecute(int peekRate, int popRate, int executions) {
		//TODO: there must be a formula for this?!
		int neededToFire = Math.max(peekRate, popRate);
		int required = 0, queued = 0;
		while (executions > 0) {
			if (queued >= neededToFire) {
				queued -= popRate;
				--executions;
			} else {
				int added = neededToFire - queued;
				queued += added;
				required += added;
			}
		}
		return required;
	}
}
