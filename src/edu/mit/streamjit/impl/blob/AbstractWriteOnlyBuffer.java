package edu.mit.streamjit.impl.blob;

/**
 * A Buffer implementation whose read methods throw
 * UnsupportedOperationException.
 *
 * This implementation overrides size() to return 0 and capacity() to return
 * Integer.MAX_VALUE, which is appropriate for Buffers for overall output, where
 * write-only buffers are most useful.  Implementations with actual capacity
 * constraints, such as a Buffer that writes into a specific array, should
 * probably throw an exception if that capacity is exceeded, rather than
 * advertising a fixed capacity, as the latter may lead to infinite loops or
 * blocking as the stream waits for space.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/21/2013
 */
public abstract class AbstractWriteOnlyBuffer extends AbstractBuffer {
	@Override
	public Object read() {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public int read(Object[] data, int offset, int length) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public boolean readAll(Object[] data) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public boolean readAll(Object[] data, int offset) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public int size() {
		return 0;
	}
	@Override
	public int capacity() {
		return Integer.MAX_VALUE;
	}
}
