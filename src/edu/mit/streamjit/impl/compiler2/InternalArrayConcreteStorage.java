package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Range;
import edu.mit.streamjit.util.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A ConcreteStorage implementation directly addressing its underlying storage
 * and that cannot be adjusted.  As its name suggests, this is most useful for
 * internal storage, where adjusts are not necessary.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class InternalArrayConcreteStorage implements ConcreteStorage {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle READ_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, InternalArrayConcreteStorage.class, "readExceptionHandler", void.class, String.class, IndexOutOfBoundsException.class, int.class);
	private static final MethodHandle WRITE_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, InternalArrayConcreteStorage.class, "writeExceptionHandler", void.class, String.class, IndexOutOfBoundsException.class, int.class, Object.class);
	private final Arrayish array;
	private final MethodHandle readHandle, writeHandle;
	public InternalArrayConcreteStorage(Arrayish array, Storage s) {
		this.array = array;
		int ssc, throughput;
		try {
			ssc = s.steadyStateCapacity();
			throughput = s.throughput();
		} catch (IllegalStateException ex) {
			ssc = throughput = -1;
		}
		String storageInfo = String.format("%s, capacity %d, throughput %d, arraylength %d%nupstream: %s%ndownstream: %s",
				s.id(), ssc, throughput, this.array.size(),
				s.upstreamGroups(),
				s.downstreamGroups());

		this.readHandle = MethodHandles.catchException(array.get(), IndexOutOfBoundsException.class,
				READ_EXCEPTION_HANDLER.bindTo(storageInfo).asType(array.get().type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
		this.writeHandle = MethodHandles.catchException(array.set(), IndexOutOfBoundsException.class,
				WRITE_EXCEPTION_HANDLER.bindTo(storageInfo).asType(array.set().type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
	}

	@Override
	public Class<?> type() {
		return array.type();
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
				Arrayish array = new Arrayish.ArrayArrayish(storage.type(), storage.steadyStateCapacity());
				return new InternalArrayConcreteStorage(array, storage);
			}
		};
	}

	public static StorageFactory initFactory(final Map<ActorGroup, Integer> initSchedule) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				Range<Integer> writeIndices = storage.writeIndexSpan(initSchedule);
				int capacity = ContiguousSet.create(writeIndices, DiscreteDomain.integers()).size();
				Arrayish array = new Arrayish.ArrayArrayish(storage.type(), capacity);
				return new InternalArrayConcreteStorage(array, storage);
			}
		};
	}
}
