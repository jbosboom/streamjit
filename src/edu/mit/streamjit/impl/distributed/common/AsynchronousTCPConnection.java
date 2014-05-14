package edu.mit.streamjit.impl.distributed.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsynchronousTCPConnection implements Connection {
	private ObjectOutputStream ooStream = null;
	private AsynchronousSocketChannel asyncSktChannel;

	private ByteBufferOutputStream bBos;
	private AtomicBoolean canWrite;

	private boolean isconnected = false;
	private final int resetCount;

	public AsynchronousTCPConnection(AsynchronousSocketChannel asyncSktChannel) {
		this(asyncSktChannel, 5000);
	}

	/**
	 * @param socket
	 * @param resetCount
	 *            reset the {@link ObjectOutputStream} after this no of sends.
	 *            To avoid out of memory error.
	 */
	public AsynchronousTCPConnection(AsynchronousSocketChannel asyncSktChannel,
			int resetCount) {
		this.resetCount = resetCount;
		try {
			this.asyncSktChannel = asyncSktChannel;

			bBos = new ByteBufferOutputStream();
			ooStream = new ObjectOutputStream(bBos);
			isconnected = true;
			canWrite = new AtomicBoolean(true);
			// System.out.println(String.format(
			// "DEBUG: TCP connection %d has been established", count++));
		} catch (IOException iex) {
			isconnected = false;
			iex.printStackTrace();
		}
	}

	// This is introduced to reduce the ooStream.reset(); frequency. Too many
	// resets, i.e., reset the ooStream for every new write severely affects the
	// performance.
	int n = 0;

	@Override
	public void writeObject(Object obj) throws IOException {
		if (isStillConnected()) {
			try {
				ooStream.writeObject(obj);
				ByteBuffer bb = bBos.getByteBuffer();

				Future<Integer> nBytes = asyncSktChannel.write(bb);
				bBos.reset();

				n++;
				// TODO: Any way to improve the performance?
				if (n > resetCount) {
					n = 0;
					ooStream.reset();
				}
				// System.out.println("Object send...");
			} catch (IOException ix) {
				// Following doesn't change when other side of the socket is
				// closed.....
				/*
				 * System.out.println("socket.isBound()" + socket.isBound());
				 * System.out.println("socket.isClosed()" + socket.isClosed());
				 * System.out.println("socket.isConnected()" +
				 * socket.isConnected());
				 * System.out.println("socket.isInputShutdown()" +
				 * socket.isInputShutdown());
				 * System.out.println("socket.isOutputShutdown()" +
				 * socket.isOutputShutdown());
				 */
				isconnected = false;
				throw ix;
			}
		} else {
			throw new IOException("TCPConnection: Socket is not connected");
		}
	}

	public final void closeConnection() {
		try {
			if (ooStream != null)
				this.ooStream.close();
			if (asyncSktChannel != null)
				this.asyncSktChannel.close();
		} catch (IOException ex) {
			isconnected = false;
			ex.printStackTrace();
		}
	}

	@Override
	public final boolean isStillConnected() {
		// return (this.socket.isConnected() && !this.socket.isClosed());
		return isconnected;
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		throw new IOException(
				"Reading object is not supported in asynchronous tcp mode");
	}

	public InetAddress getInetAddress() {
		throw new java.lang.Error("Method not Implemented");
	}

	@Override
	public void softClose() throws IOException {
		this.ooStream.write('\u001a');
		this.ooStream.flush();
	}

	public int write(Object[] data, int offset, int length) throws IOException,
			InterruptedException, ExecutionException {

		final ByteBufferOutputStream bBos;
		final ObjectOutputStream objOS;
		final AtomicBoolean canWrite;

		objOS = this.ooStream;
		bBos = this.bBos;
		canWrite = this.canWrite;

		while (!canWrite.get())
			Thread.sleep(10);

		int written = 0;
		while (written < length) {
			objOS.writeObject(data[offset++]);
			++written;
		}

		canWrite.set(false);
		ByteBuffer bb = bBos.getByteBuffer();
		asyncSktChannel.write(bb, bb,
				new CompletionHandler<Integer, ByteBuffer>() {
					@Override
					public void completed(Integer result, ByteBuffer attachment) {

						if (attachment.hasRemaining()) {
							asyncSktChannel.write(attachment, attachment, this);
						} else {
							System.out.println("Completed.. ");
							canWrite.set(true);
							bBos.reset();
							try {
								objOS.reset();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

					@Override
					public void failed(Throwable exc, ByteBuffer attachment) {
						exc.printStackTrace();
						System.out.println("**************************");
						canWrite.set(false);
					}
				});

		return written;
	}

	/**
	 * This class implements an output stream in which the data is written into
	 * a byte array. The buffer automatically grows as data is written to it.
	 * The data can be retrieved using <code>toByteArray()</code> and
	 * <code>toString()</code>.
	 * <p>
	 * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
	 * this class can be called after the stream has been closed without
	 * generating an <tt>IOException</tt>.
	 * 
	 * @author Arthur van Hoff
	 * @since JDK1.0
	 * 
	 * 
	 *        This is just a copy of {@link ByteArrayOutputStream} and just byte
	 *        array is replaced by {@link ByteBuffer} for performance.
	 * @author sumanan
	 * @since May 10, 2014
	 */
	public class ByteBufferOutputStream extends OutputStream {

		/**
		 * The buffer where data is stored.
		 */
		protected ByteBuffer bb;

		/**
		 * The number of valid bytes in the buffer.
		 */
		protected int count;

		/**
		 * Creates a new byte array output stream. The buffer capacity is
		 * initially 32 bytes, though its size increases if necessary.
		 */
		public ByteBufferOutputStream() {
			this(10 * 1024 * 1024);
		}

		public int getCount() {
			return count;
		}

		/**
		 * Creates a new byte array output stream, with a buffer capacity of the
		 * specified size, in bytes.
		 * 
		 * @param size
		 *            the initial size.
		 * @exception IllegalArgumentException
		 *                if size is negative.
		 */
		public ByteBufferOutputStream(int size) {
			if (size < 0) {
				throw new IllegalArgumentException("Negative initial size: "
						+ size);
			}
			bb = ByteBuffer.allocateDirect(size);
		}

		/**
		 * Increases the capacity if necessary to ensure that it can hold at
		 * least the number of elements specified by the minimum capacity
		 * argument.
		 * 
		 * @param minCapacity
		 *            the desired minimum capacity
		 * @throws OutOfMemoryError
		 *             if {@code minCapacity < 0}. This is interpreted as a
		 *             request for the unsatisfiably large capacity
		 *             {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}
		 *             .
		 */
		private void ensureCapacity(int minCapacity) {
			// overflow-conscious code
			if (minCapacity - bb.capacity() > 0)
				grow(minCapacity);
		}

		/**
		 * Increases the capacity to ensure that it can hold at least the number
		 * of elements specified by the minimum capacity argument.
		 * 
		 * @param minCapacity
		 *            the desired minimum capacity
		 */
		private void grow(int minCapacity) {
			// overflow-conscious code
			int oldCapacity = bb.capacity();
			int newCapacity = oldCapacity << 1;
			if (newCapacity - minCapacity < 0)
				newCapacity = minCapacity;
			if (newCapacity < 0) {
				if (minCapacity < 0) // overflow
					throw new OutOfMemoryError();
				newCapacity = Integer.MAX_VALUE;
			}
			ByteBuffer newBb = ByteBuffer.allocateDirect(newCapacity);
			newBb.clear();
			bb.flip();
			newBb.put(bb);
			bb = newBb;
		}

		/**
		 * Writes <code>len</code> bytes from the specified byte array starting
		 * at offset <code>off</code> to this output stream. The general
		 * contract for <code>write(b, off, len)</code> is that some of the
		 * bytes in the array <code>b</code> are written to the output stream in
		 * order; element <code>b[off]</code> is the first byte written and
		 * <code>b[off+len-1]</code> is the last byte written by this operation.
		 * <p>
		 * The <code>write</code> method of <code>OutputStream</code> calls the
		 * write method of one argument on each of the bytes to be written out.
		 * Subclasses are encouraged to override this method and provide a more
		 * efficient implementation.
		 * <p>
		 * If <code>b</code> is <code>null</code>, a
		 * <code>NullPointerException</code> is thrown.
		 * <p>
		 * If <code>off</code> is negative, or <code>len</code> is negative, or
		 * <code>off+len</code> is greater than the length of the array
		 * <code>b</code>, then an <tt>IndexOutOfBoundsException</tt> is thrown.
		 * 
		 * @param b
		 *            the data.
		 * @param off
		 *            the start offset in the data.
		 * @param len
		 *            the number of bytes to write.
		 * @exception IOException
		 *                if an I/O error occurs. In particular, an
		 *                <code>IOException</code> is thrown if the output
		 *                stream is closed.
		 */
		public void write(byte b[], int off, int len) throws IOException {
			if (b == null) {
				throw new NullPointerException();
			} else if ((off < 0) || (off > b.length) || (len < 0)
					|| ((off + len) > b.length) || ((off + len) < 0)) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return;
			}
			ensureCapacity(count + len);
			bb.put(b, off, len);
			count += len;
			assert count == bb.position() : "count != bb.position()";
		}

		/**
		 * Writes the specified byte to this byte array output stream.
		 * 
		 * @param b
		 *            the byte to be written.
		 */
		public synchronized void write(int b) {
			ensureCapacity(count + 1);
			bb.put((byte) b);
			count += 1;
			assert count == bb.position() : "count != bb.position()";
		}

		/**
		 * Writes <code>len</code> bytes from the specified byte array starting
		 * at offset <code>off</code> to this byte array output stream.
		 * 
		 * @param b
		 *            the data.
		 * @param off
		 *            the start offset in the data.
		 * @param len
		 *            the number of bytes to write.
		 * 
		 *            public synchronized void write(byte b[], int off, int len)
		 *            { if ((off < 0) || (off > b.length) || (len < 0) || ((off
		 *            + len) - b.length > 0)) { throw new
		 *            IndexOutOfBoundsException(); } ensureCapacity(count +
		 *            len); System.arraycopy(b, off, buf, count, len); count +=
		 *            len; }
		 */

		/**
		 * Writes the complete contents of this byte array output stream to the
		 * specified output stream argument, as if by calling the output
		 * stream's write method using <code>out.write(buf, 0, count)</code>.
		 * 
		 * @param out
		 *            the output stream to which to write the data.
		 * @exception IOException
		 *                if an I/O error occurs.
		 */
		public synchronized void writeTo(OutputStream out) throws IOException {
			out.write(getByteArray(), 0, count);
		}

		/**
		 * Resets the <code>count</code> field of this byte array output stream
		 * to zero, so that all currently accumulated output in the output
		 * stream is discarded. The output stream can be used again, reusing the
		 * already allocated buffer space.
		 * 
		 * @see java.io.ByteArrayInputStream#count
		 */
		public synchronized void reset() {
			bb.position(0);
			bb.limit(bb.capacity());
			count = 0;
		}

		/**
		 * Creates a newly allocated byte array. Its size is the current size of
		 * this output stream and the valid contents of the buffer have been
		 * copied into it.
		 * 
		 * @return the current contents of this output stream, as a byte array.
		 * @see java.io.ByteArrayOutputStream#size()
		 */
		public synchronized byte toByteArray()[] {
			return getByteArray();
		}

		/**
		 * Returns the current size of the buffer.
		 * 
		 * @return the value of the <code>count</code> field, which is the
		 *         number of valid bytes in this output stream.
		 * @see java.io.ByteArrayOutputStream#count
		 */
		public synchronized int size() {
			assert count == bb.position() : "count != bb.position()";
			return count;
		}

		/**
		 * Converts the buffer's contents into a string decoding bytes using the
		 * platform's default character set. The length of the new
		 * <tt>String</tt> is a function of the character set, and hence may not
		 * be equal to the size of the buffer.
		 * 
		 * <p>
		 * This method always replaces malformed-input and unmappable-character
		 * sequences with the default replacement string for the platform's
		 * default character set. The
		 * {@linkplain java.nio.charset.CharsetDecoder} class should be used
		 * when more control over the decoding process is required.
		 * 
		 * @return String decoded from the buffer's contents.
		 * @since JDK1.1
		 */
		public synchronized String toString() {
			return new String(getByteArray(), 0, count);
		}

		/**
		 * Converts the buffer's contents into a string by decoding the bytes
		 * using the specified {@link java.nio.charset.Charset charsetName}. The
		 * length of the new <tt>String</tt> is a function of the charset, and
		 * hence may not be equal to the length of the byte array.
		 * 
		 * <p>
		 * This method always replaces malformed-input and unmappable-character
		 * sequences with this charset's default replacement string. The
		 * {@link java.nio.charset.CharsetDecoder} class should be used when
		 * more control over the decoding process is required.
		 * 
		 * @param charsetName
		 *            the name of a supported
		 *            {@linkplain java.nio.charset.Charset </code>charset<code>}
		 * @return String decoded from the buffer's contents.
		 * @exception UnsupportedEncodingException
		 *                If the named charset is not supported
		 * @since JDK1.1
		 */
		public synchronized String toString(String charsetName)
				throws UnsupportedEncodingException {
			return new String(getByteArray(), 0, count, charsetName);
		}

		/**
		 * Creates a newly allocated string. Its size is the current size of the
		 * output stream and the valid contents of the buffer have been copied
		 * into it. Each character <i>c</i> in the resulting string is
		 * constructed from the corresponding element <i>b</i> in the byte array
		 * such that: <blockquote>
		 * 
		 * <pre>
		 * c == (char) (((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
		 * </pre>
		 * 
		 * </blockquote>
		 * 
		 * @deprecated This method does not properly convert bytes into
		 *             characters. As of JDK&nbsp;1.1, the preferred way to do
		 *             this is via the <code>toString(String enc)</code> method,
		 *             which takes an encoding-name argument, or the
		 *             <code>toString()</code> method, which uses the platform's
		 *             default character encoding.
		 * 
		 * @param hibyte
		 *            the high byte of each resulting Unicode character.
		 * @return the current contents of the output stream, as a string.
		 * @see java.io.ByteArrayOutputStream#size()
		 * @see java.io.ByteArrayOutputStream#toString(String)
		 * @see java.io.ByteArrayOutputStream#toString()
		 */
		@Deprecated
		public synchronized String toString(int hibyte) {
			return new String(getByteArray(), hibyte, 0, count);
		}

		/**
		 * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods
		 * in this class can be called after the stream has been closed without
		 * generating an <tt>IOException</tt>.
		 * <p>
		 * 
		 */
		public void close() throws IOException {
		}

		private byte[] getByteArray() {
			bb.flip();
			final int size = bb.remaining();
			byte[] buf = new byte[size];
			bb.get(buf, 0, size);
			assert count == bb.position() : "count != bb.position()";
			return buf;
		}

		public ByteBuffer getByteBuffer() {
			return bb;
		}
	}
}
