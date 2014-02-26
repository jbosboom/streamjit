package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableSortedSet;
import edu.mit.streamjit.util.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.Map;

/**
 * A ConcreteStorage backed by an array.  For internal storage only.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class InternalArrayConcreteStorage implements ConcreteStorage {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle READ_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, InternalArrayConcreteStorage.class, "readExceptionHandler", void.class, String.class, IndexOutOfBoundsException.class, int.class);
	private static final MethodHandle WRITE_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, InternalArrayConcreteStorage.class, "writeExceptionHandler", void.class, String.class, IndexOutOfBoundsException.class, int.class, Object.class);
	private final Object array;
	private final MethodHandle readHandle, writeHandle;
	private InternalArrayConcreteStorage(Storage s, int capacity) {
		this.array = Array.newInstance(s.type(), capacity);
		int ssc, throughput;
		try {
			ssc = s.steadyStateCapacity();
			throughput = s.throughput();
		} catch (IllegalStateException ex) {
			ssc = throughput = -1;
		}
		String storageInfo = String.format("%s, capacity %d, throughput %d, arraylength %d%nupstream: %s%ndownstream: %s",
				s.id(), ssc, throughput, Array.getLength(this.array),
				s.upstreamGroups(),
				s.downstreamGroups());

		MethodHandle arrayGetter = MethodHandles.arrayElementGetter(array.getClass()).bindTo(array);
		this.readHandle = MethodHandles.catchException(arrayGetter, IndexOutOfBoundsException.class,
				READ_EXCEPTION_HANDLER.bindTo(storageInfo).asType(arrayGetter.type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
		MethodHandle arraySetter = MethodHandles.arrayElementSetter(array.getClass()).bindTo(array);
		this.writeHandle = MethodHandles.catchException(arraySetter, IndexOutOfBoundsException.class,
				WRITE_EXCEPTION_HANDLER.bindTo(storageInfo).asType(arraySetter.type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
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
			throw new AssertionError(String.format("%s.read(%d, %s)", this, index), ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		try {
			writeHandle.invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.write(%d, %s)", this, index, data), ex);
		}
	}

	@Override
	public void adjust() {
		throw new AssertionError(String.format("unadjustable! %s.adjust()", this));
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

	private static void readExceptionHandler(String storageInfo, IndexOutOfBoundsException ex, int index) {
		throw new AssertionError("reading "+index+": "+storageInfo, ex);
	}

	private static void writeExceptionHandler(String storageInfo, IndexOutOfBoundsException ex, int index, Object data) {
		throw new AssertionError("writing "+data+" at "+index+": "+storageInfo, ex);
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return new InternalArrayConcreteStorage(storage, storage.steadyStateCapacity());
			}
		};
	}

	public static StorageFactory initFactory(final Map<ActorGroup, Integer> initSchedule) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				ImmutableSortedSet<Integer> writeIndices = storage.writeIndices(initSchedule);
				return new InternalArrayConcreteStorage(storage, writeIndices.isEmpty() ? 0 :
						writeIndices.last() - writeIndices.first() + 1);
			}
		};
	}
}
