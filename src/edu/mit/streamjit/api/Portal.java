package edu.mit.streamjit.api;

import edu.mit.streamjit.MessageConstraint;
import edu.mit.streamjit.impl.interp.Message;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.impl.common.Workers;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/8/2012
 */
public final class Portal<I> {
	private final Class<I> klass;
	private final List<PrimitiveWorker<?, ?>> recipients = new ArrayList<>();
	/**
	 * sender -> (recipient -> constraint).
	 */
	private final Map<PrimitiveWorker<?, ?>, Map<PrimitiveWorker<?, ?>, MessageConstraint>> constraints = new IdentityHashMap<>();
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
		recipients.add((PrimitiveWorker<?, ?>)recipient);
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
		Handle handler = new Handle(sender, recipients, constraints.get(sender));
		@SuppressWarnings("unchecked")
		I handle = (I)Proxy.newProxyInstance(klass.getClassLoader(), new Class<?>[]{klass}, handler);
		return handle;
	}

	/**
	 * Gets the list of registered recipients.  MessageConstraint needs this.
	 * @return the list of registered recipients
	 */
	/* package-private */ List<PrimitiveWorker<?, ?>> getRecipients() {
		List<PrimitiveWorker<?, ?>> retval = Collections.unmodifiableList(recipients);
		return retval;
	}

	/**
	 * Fills in our constraints map from the list of all constraints in the
	 * graph.  (We only remember constraints about us.)  Called by the
	 * interpreter after finding the constraints and before execution begins.
	 */
	/* package-private */ void setConstraints(List<MessageConstraint> allConstraints) {
		for (MessageConstraint c : allConstraints) {
			if (c.getPortal() != this)
				continue;
			Map<PrimitiveWorker<?, ?>, MessageConstraint> senderMap = constraints.get(c.getSender());
			if (senderMap == null) {
				senderMap = new IdentityHashMap<>();
				constraints.put(c.getSender(), senderMap);
			}
			senderMap.put(c.getRecipient(), c);
		}
	}

	/**
	 * The back-end of the dynamic proxy created in getHandle() (strictly
	 * speaking, this isn't the actual handle object, but HandleHandler seemed
	 * like a dumb name).
	 */
	private static class Handle implements InvocationHandler {
		private final PrimitiveWorker<?, ?> sender;
		private final List<PrimitiveWorker<?, ?>> recipients;
		/**
		 * Maps recipients to constraints for this sender.
		 */
		private final Map<PrimitiveWorker<?, ?>, MessageConstraint> constraints;
		private Handle(PrimitiveWorker<?, ?> sender, List<PrimitiveWorker<?, ?>> recipients, Map<PrimitiveWorker<?, ?>, MessageConstraint> constraints) {
			this.sender = sender;
			this.recipients = recipients;
			this.constraints = constraints;
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

			for (PrimitiveWorker<?, ?> recipient : recipients) {
				MessageConstraint constraint = constraints.get(recipient);
				assert constraint != null;

				//Queue up the message at the recipient.
				Message message = new Message(method, args, constraint.getDeliveryTime(Workers.getExecutions(sender)));
				Workers.sendMessage(recipient, message);
			}

			//Methods on the portal interface return void.
			return null;
		}
	}

	//<editor-fold defaultstate="collapsed" desc="Friend pattern support (see impl.common.Portals)">
	private static class PortalsFriend extends edu.mit.streamjit.impl.common.Portals {
		@Override
		protected List<PrimitiveWorker<?, ?>> getRecipients_impl(Portal<?> portal) {
			return portal.getRecipients();
		}
		@Override
		protected void setConstraints_impl(Portal<?> portal, List<MessageConstraint> constraints) {
			portal.setConstraints(constraints);
		}
		private static void init() {
			edu.mit.streamjit.impl.common.Portals.setFriend(new PortalsFriend());
		}
	}
	static {
		PortalsFriend.init();
	}
	//</editor-fold>
}
