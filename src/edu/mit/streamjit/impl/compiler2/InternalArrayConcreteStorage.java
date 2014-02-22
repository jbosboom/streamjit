package edu.mit.streamjit.impl.compiler2;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;

/**
 * A ConcreteStorage backed by an array.  For internal storage only.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class InternalArrayConcreteStorage implements ConcreteStorage {
	private final Object array;
	private final MethodHandle readHandle, writeHandle;
	private InternalArrayConcreteStorage(Storage s, int capacity) {
		this.array = Array.newInstance(s.type(), capacity);
		this.readHandle = MethodHandles.arrayElementGetter(array.getClass()).bindTo(array);
		this.writeHandle = MethodHandles.arrayElementSetter(array.getClass()).bindTo(array);
	}

	@Override
	public Class<?> type() {
		return array.getClass().getComponentType();
	}

	@Override
	public Object read(int index) {
		try {
			return readHandle.invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		try {
			writeHandle.invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public void adjust() {
		throw new AssertionError("don't adjust "+getClass().getSimpleName());
	}

	@Override
	public void sync() {
	}

	@Override
	public MethodHandle readHandle() {
		return readHandle;
	}

	@Override
	public MethodHandle writeHandle() {
		return writeHandle;
	}

	@Override
	public MethodHandle adjustHandle() {
		throw new AssertionError("don't adjust "+getClass().getSimpleName());
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return new InternalArrayConcreteStorage(storage, storage.steadyStateCapacity());
			}
		};
	}
}
