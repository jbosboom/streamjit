package org.mit.jstreamit;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

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

			for (I recipient : recipients) {
				//TODO: Compute latency between sender and recipient and queue message.
			}

			//Methods on the portal interface return void.
			return null;
		}
	}
}
