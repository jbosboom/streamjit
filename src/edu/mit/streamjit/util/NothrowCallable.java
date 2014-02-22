package edu.mit.streamjit.util;

import java.util.concurrent.Callable;

/**
 * A Callable that does not throw checked exceptions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/21/2014
 */
public interface NothrowCallable<V> extends Callable<V> {
	@Override
	public V call();
}
