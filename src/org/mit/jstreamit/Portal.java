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
	private final List<I> listeners = new ArrayList<>();
	public Portal(Class<I> klass) {
		this.klass = klass;
	}

	public void addListener(I listener) {
		//I'm pretty sure this can only happen via unchecked casts or
		//incompatible class file changes, but we should check anyway.
		if (!klass.isInstance(listener))
			throw new IllegalArgumentException("Listener "+listener+" not instance of "+klass);
		listeners.add(listener);
	}

	public I getHandle(int latency) {
		//When running in the JIT compiler, this is a JIT hook instead.
		return (I)Proxy.newProxyInstance(klass.getClassLoader(), new Class[]{klass}, new LatencyInvocationHandler(latency));
	}

	private class LatencyInvocationHandler implements InvocationHandler {
		private final int latency;
		LatencyInvocationHandler(int latency) {
			this.latency = latency;
		}
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			throw new UnsupportedOperationException("TODO");
		}
	}
}
