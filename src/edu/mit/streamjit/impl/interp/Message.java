package edu.mit.streamjit.impl.interp;

import java.lang.reflect.Method;
import java.util.Arrays;

public class Message implements Comparable<Message> {
	public final Method method;
	public final Object[] args;
	/**
	 * The execution immediately before which this message will be received.
	 */
	public long timeToReceive;

	public Message(Method method, Object[] args, long timeToReceive) {
		this.method = method;
		this.args = args;
		this.timeToReceive = timeToReceive;
	}

	@Override
	public int compareTo(Message o) {
		return Long.compare(timeToReceive, o.timeToReceive);
	}

	@Override
	public String toString() {
		String argString = Arrays.toString(args);
		argString = argString.substring(1, argString.length() - 1);
		return method.getDeclaringClass().getSimpleName() + "." + method.getName() + "(" + argString + ") at " + timeToReceive;
	}
}
