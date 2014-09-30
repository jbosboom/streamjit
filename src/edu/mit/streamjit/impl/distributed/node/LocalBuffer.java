package edu.mit.streamjit.impl.distributed.node;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.util.ConstructorSupplier;
import edu.mit.streamjit.util.ReflectionUtils;

/**
 * {@link LocalBuffer} connects up blob and down blob where both are running at
 * same StreamNode. In this case, we can simply use any {@link Buffer}
 * implementation to connect up blob and down blob. But at the draining time
 * blobs write large amount of data, which blobs buffered inside during the init
 * schedule, and limited buffer size causes deadlock. Implementations of the
 * {@link LocalBuffer} are supposed to understand drainingStarted event and
 * increase the buffer size smartly to avoid deadlock situation. see Deadlock 5.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Sept 23, 2014
 */
public interface LocalBuffer extends Buffer {

	public void drainingStarted(DrainType drainType);

	/**
	 * Just a wrapper for {@link ConcurrentArrayBuffer}
	 */
	public class ConcurrentArrayLocalBuffer implements LocalBuffer {
		private final ConcurrentArrayBuffer buffer;

		public ConcurrentArrayLocalBuffer(int capacity) {
			buffer = new ConcurrentArrayBuffer(capacity);
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

		@Override
		public void drainingStarted(DrainType drainType) {
			System.out.println("drainingStarted: Not supported");
		}
	}

	/**
	 * Modified version of {@link DynamicBufferManager#DynamicBuffer}. Instead
	 * of dynamically increase the buffer sizes, this implementation creates an
	 * additional buffer to unlock the draining time deadlock.
	 * 
	 * 
	 * Blobs write more than expected amount of data during the draining time
	 * and, sometimes the output buffers become full forever at the draining
	 * time and blobs spin on write() forever. This implementation creates new a
	 * supplied buffer in order to avoid dead locks during draining time.
	 * 
	 * <p>
	 * Determining whether buffer fullness is due to deadlock situation or the
	 * current blob is executing on a faster node than the down stream blob is
	 * little tricky.
	 * </p>
	 * 
	 * <p>
	 * TODO: {@link ConstructorSupplier} can be reused here to instantiate the
	 * buffer instances if we make {@link ConstructorSupplier}.arguments not
	 * final.
	 * </p>
	 * 
	 */
	public class LocalBuffer1 implements LocalBuffer {

		private final int capacityPos;

		private final Constructor<? extends Buffer> cons;

		private final Buffer defaultBuffer;

		private volatile Buffer drainBuffer;

		/**
		 * Minimum time gap between the last successful write and the current
		 * time in order to consider the option of doubling the buffer
		 */
		private final long gap;

		private volatile boolean hasDrainingStarted;

		private final List<?> initialArguments;

		private final int initialCapacity;

		/**
		 * Every successful write operation should update this time.
		 */
		private long lastWrittenTime;

		private final String name;

		private Buffer writeBuffer;

		public LocalBuffer1(String name, Class<? extends Buffer> bufferClass,
				List<?> initialArguments, int initialCapacity, int capacityPos) {
			this.name = name;
			this.initialArguments = initialArguments;
			this.initialCapacity = initialCapacity;
			this.capacityPos = capacityPos;
			Constructor<? extends Buffer> con = null;
			try {
				con = ReflectionUtils.findConstructor(bufferClass,
						initialArguments);
			} catch (NoSuchMethodException e1) {
				e1.printStackTrace();
			}
			this.cons = con;
			this.defaultBuffer = getNewBuffer(initialCapacity);
			this.writeBuffer = defaultBuffer;
			this.gap = 10_000_000_000l; // 10s
			hasDrainingStarted = false;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see edu.mit.streamjit.impl.blob.Buffer#capacity()
		 * 
		 * if hasDrainingStarted == true sends defaultBuffer.capacity() * 2 as
		 * capacity because if the room is 0 blobs never try to call write and
		 * hence wirteFailed() will never be called. Check
		 * Interpreter#pushOutputs().
		 */
		@Override
		public int capacity() {
			int cap;
			if (hasDrainingStarted) {
				cap = drainBuffer == null ? defaultBuffer.capacity() * 2
						: drainBuffer.capacity() + defaultBuffer.capacity();
			} else
				cap = defaultBuffer.capacity();
			return cap;
		}

		@Override
		public void drainingStarted(DrainType drainType) {
			hasDrainingStarted = true;
		}

		@Override
		public Object read() {
			Object o;
			if (drainBuffer == null || defaultBuffer.size() > 0)
				o = defaultBuffer.read();
			else
				o = drainBuffer.read();
			return o;
		}

		@Override
		public int read(Object[] data, int offset, int length) {
			int ret;
			if (drainBuffer == null)
				ret = defaultBuffer.read(data, offset, length);
			else if (defaultBuffer.size() == 0)
				ret = drainBuffer.read(data, offset, length);
			else {
				ret = defaultBuffer.read(data, offset, length);
				ret += drainBuffer.read(data, offset + ret, length - ret);
			}
			return ret;
		}

		@Override
		public boolean readAll(Object[] data) {
			return readAll(data, 0);
		}

		@Override
		public boolean readAll(Object[] data, int offset) {
			boolean ret;
			int need = data.length - offset;
			if (drainBuffer == null)
				ret = defaultBuffer.readAll(data, offset);
			else if (defaultBuffer.size() == 0)
				ret = drainBuffer.readAll(data, offset);
			else if (need > defaultBuffer.size() + drainBuffer.size())
				ret = false;
			else {
				int read = defaultBuffer.read(data, offset, need);
				read += drainBuffer.read(data, offset + read, need - read);
				ret = true;
				if (read != need)
					throw new IllegalStateException(
							"data buffer is not full. Check the logic");
			}
			return ret;
		}

		@Override
		public int size() {
			int size = drainBuffer == null ? defaultBuffer.size() : drainBuffer
					.size() + defaultBuffer.size();
			return size;
		}

		@Override
		public boolean write(Object t) {
			boolean ret = writeBuffer.write(t);
			if (!ret)
				writeFailed();
			return ret;
		}

		@Override
		public int write(Object[] data, int offset, int length) {
			int written = writeBuffer.write(data, offset, length);
			if (written != length) {
				writeFailed();
				written += writeBuffer.write(data, offset + written, length
						- written);
			}
			return written;
		}

		private void createDrainBuffer() {
			assert drainBuffer == null : "drainBuffer has already been created.";
			int newCapacity = 4 * defaultBuffer.capacity();
			System.out
					.println(String
							.format("%s : Creating drain buffer: defaultBufferCapacity - %d, drainBufferCapacity - %d",
									name, initialCapacity, newCapacity));
			drainBuffer = getNewBuffer(newCapacity);
			this.writeBuffer = drainBuffer;
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

		private Buffer getNewBuffer(int newCapacity) {
			Buffer buffer;
			try {
				buffer = cons.newInstance(getArguments(newCapacity).toArray());
				return buffer;
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException e) {
				e.printStackTrace();
				return null;
			}
		}

		private void writeFailed() {
			if (!hasDrainingStarted)
				return;

			if (drainBuffer != null)
				throw new IllegalStateException(
						String.format(
								"drainBuffer is full "
										+ "drainBuffer.size() = %d, defaultBuffer.size() = %d  ",
								drainBuffer.size(), defaultBuffer.size()));
			createDrainBuffer();
		}
	}
}
