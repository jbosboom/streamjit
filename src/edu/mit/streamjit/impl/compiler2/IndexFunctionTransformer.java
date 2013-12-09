package edu.mit.streamjit.impl.compiler2;

import java.lang.invoke.MethodHandle;
import java.util.NavigableSet;

/**
 * Transforms index functions into equivalent (but possibly more performant)
 * handles.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/8/2013
 */
public interface IndexFunctionTransformer {
	/**
	 * Returns a handle equivalent to the given handle when evaluated over the
	 * given domain.
	 * @param fxn the function to transform
	 * @param domain the domain the function will be evaluated over
	 * @return an equivalent handle
	 */
	public MethodHandle transform(MethodHandle fxn, NavigableSet<Integer> domain);
}
