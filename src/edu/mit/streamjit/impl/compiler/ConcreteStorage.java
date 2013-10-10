package edu.mit.streamjit.impl.compiler;

import java.lang.invoke.MethodHandle;

/**
 * An actual buffer or other storage element.  (Contrast with Storage, which is
 * an IR-level object.)
 *
 * Physical indices are adjusted by this storage to account for e.g. circular
 * buffers, so they might be considered "logical physical" indices.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public interface ConcreteStorage {
	/**
	 * Returns the type of elements stored in this ConcreteStorage.
	 * @return the type of elements stored in this ConcreteStorage
	 */
	public Class<?> type();
	/**
	 * Returns a MethodHandle of int -> T type, where T is the type of elements
	 * stored in this storage, that maps a physical index to the element stored
	 * at that index.
	 * @return a handle that reads from this storage
	 */
	public MethodHandle readHandle();
	/**
	 * Returns a MethodHandle of int, T -> void type, where T is the type of
	 * elements stored in this storage, that stores an element at the given
	 * physical index.
	 * @return a handle that writes to this storage
	 */
	public MethodHandle writeHandle();
	/**
	 * Returns a MethodHandle of void -> void type that performs any
	 * end-of-steady-state adjustments required by this storage.  (For example,
	 * a circular buffer will increment its head and tail indices.)
	 * @return a handle that adjusts this storage
	 */
	public MethodHandle adjustHandle();
}
