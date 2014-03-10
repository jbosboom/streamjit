package edu.mit.streamjit.impl.distributed.node;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.util.ConstructorSupplier;
import edu.mit.streamjit.util.ReflectionUtils;

/**
 * <b> Use a {@link DynamicBufferManager} per blob ( i.e., one to one mapping
 * between a blob and a DynamicBufferManager). Do not use same instance of
 * {@link DynamicBufferManager} to create buffers for multiple blobs. </b>
 * 
 * @author sumanan
 * @since Mar 10, 2014
 * 
 */
public final class DynamicBufferManager {

	/**
	 * keep track of all buffers created for a particular blob.
	 */
	private List<Buffer> buffers;

	public DynamicBufferManager() {
		buffers = new ArrayList<>();
	}

	/**
	 * Make and return a dynamic buffer backed by an instance of bufferClass.
	 * 
	 * @param bufferClass
	 *            : Any concrete implementation of {@link Buffer}.
	 * @param initialArguments
	 *            : Constructor arguments. : Initial capacity of the buffer.
	 * @param capacityPos
	 *            : the position of size parameter in the bufferClass.
	 * @return : A dynamic buffer backed by an instance of bufferClass.
	 */
	public Buffer getBuffer(Class<? extends Buffer> bufferClass,
			List<?> initialArguments, int initialCapacity, int capacityPos) {
		Buffer buf = new DynamicBuffer(bufferClass, initialArguments,
				initialCapacity, capacityPos);
		buffers.add(buf);
		return buf;
	}

	/**
	 * Dynamically increases supplied buffer capacity in order to avoid dead
	 * locks. Actually creates a new instance of the supplied buffer and copy
	 * the data from old buffer to new buffer. A decorator pattern for
	 * {@link Buffer}.
	 * 
	 * <p>
	 * Determining whether buffer is full due to deadlock situation or the
	 * current blob is executing on a faster node than the down stream blob is
	 * little tricky. Here we assume blobs are running on nearly equivalent.
	 * </p>
	 * 
	 * <p>
	 * TODO: {@link ConstructorSupplier} can be reused here to instantiate the
	 * buffer instances if we make {@link ConstructorSupplier}.arguments not
	 * final.
	 * </p>
	 * 
	 * <p>
	 * TODO: Possible performance bug during read() due to volatile buffer
	 * variable and the need for acquire readLock for every single reading. Any
	 * way to improve this???
	 * 
	 * @author sumanan
	 * @since Mar 10, 2014
	 * 
	 */
	private class DynamicBuffer implements Buffer {

		/**
		 * Minimum time gap between the last successful write and the current
		 * time in order to consider the option of doubling the buffer
		 */
		private final long gap;

		/**
		 * Every successful write operation should update this time.
		 */
		private long lastWrittenTime;

		/**
		 * When the algorithm detects there are some progress ( May be after
		 * some expansions), this flag is set to stop any future expansions.
		 * This is to prevent infinity buffer growth.
		 */
		private boolean expandable;

		/**
		 * Read lock should be acquired at every single read where as write lock
		 * will be acquired only when switching the buffer from old to new.
		 */
		private ReadWriteLock rwlock;

		private final Class<? extends Buffer> bufferClass;
		private final List<?> initialArguments;
		private final int initialCapacity;
		private final int capacityPos;
		private final Constructor<? extends Buffer> cons;

		/**
		 * TODO: Volatileness may severely affects the reading performance.
		 */
		private volatile Buffer buffer;

		public DynamicBuffer(Class<? extends Buffer> bufferClass,
				List<?> initialArguments, int initialCapacity, int capacityPos) {
			this.bufferClass = bufferClass;
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
			this.buffer = getNewBuffer(initialCapacity);
			this.gap = 200000000; // 200ms
			expandable = true;
			rwlock = new ReentrantReadWriteLock();
			lastWrittenTime = 0;
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

		@Override
		public Object read() {
			rwlock.readLock().lock();
			Object o = buffer.read();
			rwlock.readLock().unlock();
			return o;
		}

		@Override
		public int read(Object[] data, int offset, int length) {
			rwlock.readLock().lock(); // TODO: getting lock for each single
										// read. Severely affect performance.
			int ret = buffer.read(data, offset, length);
			rwlock.readLock().unlock();
			return ret;
		}

		@Override
		public boolean readAll(Object[] data) {
			rwlock.readLock().lock();
			boolean ret = buffer.readAll(data);
			rwlock.readLock().unlock();
			return ret;
		}

		@Override
		public boolean readAll(Object[] data, int offset) {
			rwlock.readLock().lock();
			boolean ret = buffer.readAll(data, offset);
			rwlock.readLock().unlock();
			return ret;
		}

		@Override
		public boolean write(Object t) {
			boolean ret = buffer.write(t);
			if (!ret)
				writeFailed();
			else if (lastWrittenTime != 0)
				lastWrittenTime = 0;
			return ret;
		}

		@Override
		public int write(Object[] data, int offset, int length) {
			int written = buffer.write(data, offset, length);
			if (written == 0)
				writeFailed();
			else if (lastWrittenTime != 0)
				lastWrittenTime = 0;
			return written;
		}

		@Override
		public int size() {
			return buffer.size();
		}

		@Override
		public int capacity() {
			return buffer.capacity();
		}

		private void writeFailed() {
			if (areAllFull() || !expandable)
				return;

			if (lastWrittenTime == 0) {
				lastWrittenTime = System.nanoTime();
				return;
			}

			if (System.nanoTime() - lastWrittenTime > gap && expandable) {
				doubleBuffer();
			}
		}

		private boolean areAllFull() {
			for (Buffer b : buffers) {
				if (b.size() != b.capacity())
					return false;
			}
			return true;
		}

		private void doubleBuffer() {
			int newCapacity = 2 * buffer.capacity();
			if (newCapacity > 1024 * initialCapacity) {
				expandable = false;
				return;
			}
			System.out
					.println(String
							.format("Doubling the buffer: newCapacity - %d, initialCapacity - %d",
									newCapacity, initialCapacity));
			Buffer newBuf = getNewBuffer(newCapacity);
			rwlock.writeLock().lock();
			final int size = buffer.size();
			// TODO: copying is done one by one. Any block level copying?
			for (int i = 0; i < size; i++) {
				newBuf.write(buffer.read());
			}

			// System.out.println("buffer size after copying = " +
			// buffer.size());
			if (buffer.size() != 0) {
				throw new IllegalStateException(
						"Buffter is not empty after copying all data");
			}
			this.buffer = newBuf;
			lastWrittenTime = 0;
			rwlock.writeLock().unlock();
		}
	}
}
