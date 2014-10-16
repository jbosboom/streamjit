package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Supplier;
import java.lang.invoke.MethodHandle;
import java.util.NavigableSet;

/**
 * An IndexFunctionTransformer that simply returns the input function.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/8/2013
 */
public class IdentityIndexFunctionTransformer implements IndexFunctionTransformer {
	@Override
	public MethodHandle transform(MethodHandle fxn, Supplier<? extends NavigableSet<Integer>> domain) {
		return fxn;
	}
	@Override
	public boolean equals(Object obj) {
		return getClass() == obj.getClass();
	}
	@Override
	public int hashCode() {
		return 0;
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
