package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.checkArgument;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import static edu.mit.streamjit.util.LookupUtils.findVirtual;
import edu.mit.streamjit.util.NIOBufferUtils;
import edu.mit.streamjit.util.PrimitiveUtils;
import java.io.Serializable;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.util.Locale;

/**
 * An array-ish object; that is, storage for items of a given type (and its
 * subtypes) in slots from 0 to size-1.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/28/2014
 */
public interface Arrayish {
	public static final MethodHandle GET = findVirtual(MethodHandles.publicLookup(), Arrayish.class, "get", Object.class, int.class);
	public static final MethodHandle SET = findVirtual(MethodHandles.publicLookup(), Arrayish.class, "set", void.class, int.class, Object.class);
	public Class<?> type();
	public int size();
	/**
	 * Returns a MethodHandle of type int -> T.
	 * @return a read handle
	 */
	public MethodHandle get();
	/**
	 * Returns a MethodHandle of type int, T -> void.
	 * @return a write handle
	 */
	public MethodHandle set();

	/**
	 * A Factory for Arrayish objects.
	 *
	 * TODO: Java 8: can probably be replaced with constructor references.
	 */
	public interface Factory extends Serializable {
		public Arrayish make(Class<?> type, int size);
	}

	/**
	 * An Arrayish backed by an actual Java array.
	 */
	public static final class ArrayArrayish implements Arrayish {
		private final Object array;
		private final MethodHandle get, set;
		public ArrayArrayish(Class<?> type, int size) {
			this.array = Array.newInstance(type, size);
			this.get = MethodHandles.arrayElementGetter(array.getClass()).bindTo(array);
			this.set = MethodHandles.arrayElementSetter(array.getClass()).bindTo(array);
		}
		@Override
		public Class<?> type() {
			return array.getClass().getComponentType();
		}
		@Override
		public int size() {
			return Array.getLength(array);
		}
		@Override
		public MethodHandle get() {
			return get;
		}
		@Override
		public MethodHandle set() {
			return set;
		}
		public static Factory factory() {
			return new Factory() {
				private static final long serialVersionUID = 1L;
				@Override
				public Arrayish make(Class<?> type, int size) {
					return new ArrayArrayish(type, size);
				}
			};
		}
	}

	/**
	 * An Arrayish of primitives backed by a direct NIO buffer.  (Non-direct NIO
	 * buffers are just arrays -- use ArrayArrayish instead.)
	 */
	public static final class NIOArrayish implements Arrayish {
		private final Buffer buffer;
		private final int size;
		private final MethodHandle get, set;
		public NIOArrayish(Class<?> type, int size) {
			checkArgument(type.isPrimitive() && !type.equals(void.class), "%s can't be stored in an NIO buffer", type);
			Class<?> dataType = type.equals(boolean.class) ? byte.class : type;
			Class<? extends Buffer> bufferType = NIOBufferUtils.bufferForPrimitive(dataType);
			this.buffer = NIOBufferUtils.allocateDirect(bufferType, size);
			this.size = size;

			//explicitCastArguments converts byte to boolean and back; otherwise
			//the types exactly match and the target is returned immediately.
			this.get = MethodHandles.explicitCastArguments(
					findVirtual(MethodHandles.publicLookup(), bufferType, "get", dataType, int.class)
							.bindTo(buffer),
					MethodType.methodType(type, int.class));
			this.set = MethodHandles.explicitCastArguments(
					findVirtual(MethodHandles.publicLookup(), bufferType, "put", bufferType, int.class, dataType)
							.bindTo(buffer),
					MethodType.methodType(void.class, int.class, type));
		}
		@Override
		public Class<?> type() {
			return get.type().returnType();
		}
		@Override
		public int size() {
			return size;
		}
		@Override
		public MethodHandle get() {
			return get;
		}
		@Override
		public MethodHandle set() {
			return set;
		}
		public static Factory factory() {
			return new Factory() {
				private static final long serialVersionUID = 1L;
				@Override
				public Arrayish make(Class<?> type, int size) {
					return new NIOArrayish(type, size);
				}
			};
		}
	}

	/**
	 * An Arrayish of primitives backed by native memory.  The returned handles
	 * do not keep a strong reference to this object, so something else must do
	 * so to prevent the native memory from being freed while it's still used.
	 * (In Compiler2, Read/Write/DrainInstructions that reference
	 * ConcreteStorage serve this function.)
	 */
	public static final class UnsafeArrayish implements Arrayish {
		private static final sun.misc.Unsafe UNSAFE;
		static {
			try {
				Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
				f.setAccessible(true);
				UNSAFE = (sun.misc.Unsafe)f.get(null);
			} catch (NoSuchFieldException | IllegalAccessException ex) {
				throw new AssertionError(ex);
			}
		}
		private static final MethodHandle INDEX = findStatic(MethodHandles.lookup(), UnsafeArrayish.class, "index", long.class, long.class, int.class, int.class);
		private final long memory;
		private final int size;
		private final MethodHandle get, set;
		public UnsafeArrayish(Class<?> type, int size) {
			//We can't store object references for lack of GC roots.
			checkArgument(type.isPrimitive() && !type.equals(void.class), "%s can't be stored in native memory", type);
			checkArgument(size >= 0, "bad size: %s", size);
			this.memory = UNSAFE.allocateMemory(size * PrimitiveUtils.sizeof(type));
			this.size = size;

			Class<?> dataType = type.equals(boolean.class) ? byte.class : type;
			String dataTypeNameCap = dataType.getSimpleName().substring(0, 1).toUpperCase(Locale.ROOT)
					+ dataType.getSimpleName().substring(1);
			MethodHandle index = MethodHandles.insertArguments(INDEX, 0, memory, PrimitiveUtils.sizeof(dataType));
			//explicitCastArguments converts byte to boolean and back; otherwise
			//the types exactly match and the target is returned immediately.
			this.get = MethodHandles.explicitCastArguments(
					MethodHandles.filterArguments(
							findVirtual(MethodHandles.publicLookup(), UNSAFE.getClass(), "get" + dataTypeNameCap, dataType, long.class)
									.bindTo(UNSAFE),
							0, index),
					MethodType.methodType(type, int.class));
			this.set = MethodHandles.explicitCastArguments(
					MethodHandles.filterArguments(
							findVirtual(MethodHandles.publicLookup(), UNSAFE.getClass(), "put" + dataTypeNameCap, void.class, long.class, dataType)
									.bindTo(UNSAFE),
							0, index),
					MethodType.methodType(void.class, int.class, type));
		}
		@Override
		public Class<?> type() {
			return get.type().returnType();
		}
		@Override
		public int size() {
			return size;
		}
		@Override
		public MethodHandle get() {
			return get;
		}
		@Override
		public MethodHandle set() {
			return set;
		}
		private static long index(long base, int stride, int index) {
			return base + stride * index;
		}
		@Override
		protected void finalize() throws Throwable {
			super.finalize();
			UNSAFE.freeMemory(memory);
		}
		public static Factory factory() {
			return new Factory() {
				private static final long serialVersionUID = 1L;
				@Override
				public Arrayish make(Class<?> type, int size) {
					return new UnsafeArrayish(type, size);
				}
			};
		}
	}
}
