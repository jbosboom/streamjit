package edu.mit.streamjit.impl.blob;

/**
 * A Buffer implementation whose write methods throw
 * UnsupportedOperationException.  Note that a read-only buffer is not immutable
 * because reading consumes elements from it, changes its size(), etc.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/21/2013
 */
public abstract class AbstractReadOnlyBuffer extends AbstractBuffer {
	@Override
	public boolean write(Object t) {
		throw new UnsupportedOperationException("read-only buffer");
	}
	@Override
	public int write(Object[] data, int offset, int length) {
		throw new UnsupportedOperationException("read-only buffer");
	}
	@Override
	public int capacity() {
		//The only reason to check a buffer's capacity is to see if there's room
		//to write into it, but we're read-only, so it shouldn't matter.  But
		//we need to return something constant and at least as big as size.
		return Integer.MAX_VALUE;
	}
}
