package edu.mit.streamjit.impl.distributed.node;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * Dynamically increases supplied buffer capacity in order to avoid dead locks.
 * Actually creates a new instance of the supplied buffer and copy the data from
 * old buffer to new buffer. A decorator pattern for {@link Buffer}.
 * 
 * <p>
 * Determining whether buffer is full due to deadlock situation or the current
 * blob is executing on a faster node than the down stream blob is little
 * tricky. Here we assume blobs are running on nearly equivalent
 * 
 * @author sumanan
 * @since Mar 10, 2014
 * 
 */
public class DynamicBuffer implements Buffer {
	
	
	private Buffer buffer;
	private final List<?> initialArguments;
	private final int initialCapacity;
	private final int capacityPos;
	private final Class<? extends Buffer> bufferClass;
	private final Constructor<? extends Buffer> cons;

	public DynamicBuffer(Class<? extends Buffer> bufferClass,
			List<?> initialArguments, int initialCapacity, int capacityPos)
			throws NoSuchMethodException, SecurityException {
		this.bufferClass = bufferClass;
		this.initialArguments = initialArguments;
		this.initialCapacity = initialCapacity;
		this.capacityPos = capacityPos;
		this.cons = bufferClass.getConstructor((Class<?>[]) initialArguments
				.toArray());
		this.buffer = getNewBuffer(getArguments(initialCapacity));
	}

	private List<?> getArguments(int newCapacity) {
		List<Object> newArgs = new ArrayList<>(initialArguments.size());
		for (int i = 0; i < initialArguments.size(); i++) {
			if (i == capacityPos)
				newArgs.add(newCapacity);
			else
				newArgs.add(initialArguments.get(i));
		}
		return newArgs;
	}

	private Buffer getNewBuffer(List<?> args) {
		Buffer buffer;
		try {
			buffer = cons.newInstance(args);
			return buffer;
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	public Object read() {
		return buffer.read();
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		return buffer.read(data, offset, length);
	}

	@Override
	public boolean readAll(Object[] data) {
		return buffer.readAll(data);
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		return buffer.readAll(data, offset);
	}

	@Override
	public boolean write(Object t) {
		return buffer.write(t);
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		return buffer.write(data, offset, length);
	}

	@Override
	public int size() {
		return buffer.size();
	}

	@Override
	public int capacity() {
		return buffer.capacity();
	}
}
