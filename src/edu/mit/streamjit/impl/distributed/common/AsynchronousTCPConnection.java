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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import edu.mit.streamjit.impl.blob.AbstractWriteOnlyBuffer;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * Uses {@link AsynchronousSocketChannel} from Java's NIO.2 to send data. This
 * class only supports bulk asynchronous write. Reads ({@link #readObject()}) or
 * single object writes ({@link #writeObject(Object)}) are not supported.
 * Serialises object array into {@link ByteBuffer} and sends it over a
 * {@link AsynchronousSocketChannel}. Further, for the performance purposes, in
 * oder to parallelise serialisation task and sending task, multiple
 * {@link ByteBuffer}s are used. So that while user thread is serialising the
 * data into a {@link ByteBuffer}, Java threads can send the already written
 * bytebuffers.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 05, 2014
 * 
 */
public class AsynchronousTCPConnection implements Connection {
	/**
	 * Backed by {@link ByteBufferArrayOutputStream}.
	 */
	private ObjectOutputStream ooStream = null;

	private AsynchronousSocketChannel asyncSktChannel;

	private ByteBufferArrayOutputStream bBAos;

	private boolean isconnected = false;

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
		try {
			this.asyncSktChannel = asyncSktChannel;

			bBAos = new ByteBufferArrayOutputStream(2);
			ooStream = new ObjectOutputStream(bBAos);
			isconnected = true;
		} catch (IOException iex) {
			isconnected = false;
			iex.printStackTrace();
		}
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		throw new java.lang.Error("Method not Implemented");
		/*
		 * if (isStillConnected()) {
		 * 
		 * while (!canWrite.get()) ;
		 * 
		 * try { ooStream.writeObject(obj); send(); } catch (IOException ix) {
		 * isconnected = false; throw ix; } } else { throw new
		 * IOException("TCPConnection: Socket is not connected"); }
		 */
	}

	public int write(Object[] data, int offset, int length) throws IOException {

		final ObjectOutputStream objOS = this.ooStream;
		final ByteBufferArrayOutputStream bBAos = this.bBAos;

		int written = 0;
		if (bBAos.newWrite()) {
			while (written < length) {
				objOS.writeObject(data[offset++]);
				++written;
			}
			objOS.reset();
			bBAos.writeCompleted();
		}

		send();
		return written;
	}

	private void send() {
		final ByteBufferOutputStream bBos;
		final ByteBufferArrayOutputStream bBAos;

		bBAos = this.bBAos;

		bBos = bBAos.newRead();
		if (bBos == null)
			return;

		ByteBuffer bb = bBos.getByteBuffer();
		bb.flip();
		asyncSktChannel.write(bb, bb,
				new CompletionHandler<Integer, ByteBuffer>() {
					@Override
					public void completed(Integer result, ByteBuffer attachment) {

						if (attachment.hasRemaining()) {
							asyncSktChannel.write(attachment, attachment, this);
						} else {
							bBAos.readCompleted();
							send();
						}
					}

					@Override
					public void failed(Throwable exc, ByteBuffer attachment) {
						isconnected = false;
						exc.printStackTrace();
					}
				});
	}

	public final void closeConnection() {
		isconnected = false;
		try {
			if (ooStream != null)
				this.ooStream.close();
			if (asyncSktChannel != null)
				this.asyncSktChannel.close();
		} catch (IOException ex) {
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
		throw new java.lang.Error(
				"Reading object is not supported in asynchronous tcp mode");
	}

	@Override
	public void softClose() throws IOException {
		while (!bBAos.newWrite())
			;
		this.ooStream.write('\u001a');
		this.ooStream.flush();
		bBAos.writeCompleted();
		send();
		System.err.println("Softclose is called");
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
	 *        This is a copy of {@link ByteArrayOutputStream} and byte array in
	 *        ByteArrayOutputStream is replaced by {@link ByteBuffer} for
	 *        performance.
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
			System.out.println("Growing bytebuffer. newCapacity = "
					+ newCapacity);
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

	/**
	 * A {@link ByteBufferOutputStream} ( implicitly {@link ByteBuffer} ) can be
	 * in one of following 4 state. State of a {@link ByteBufferOutputStream}
	 * expected to change in a cyclic manner, from canWrite -> beingWritten ->
	 * canRead -> beingRead -> canWrite.
	 * 
	 * @author sumanan
	 */
	private enum Status {
		canWrite, beingWritten, canRead, beingRead
	}

	/**
	 * Writers must call {@link #newWrite()} before begins the write process and
	 * call {@link #writeCompleted()} after the end of write process. Whatever
	 * written in between these two calls will be captured into single
	 * {@link ByteBufferOutputStream}.
	 * 
	 * Like writers, readers also call {@link #newRead()} to get the current
	 * {@link ByteBufferOutputStream} to read and must call
	 * {@link #readCompleted()} after the end of read process.
	 * 
	 * @author sumanan
	 * 
	 */
	public class ByteBufferArrayOutputStream extends OutputStream {

		private final int debugPrint;

		/**
		 * Read index of {@link #bytebufferArray}.
		 */
		private int readIndex;

		/**
		 * Write index of {@link #bytebufferArray}.
		 */
		private int writeIndex;

		private final ByteBufferOutputStream[] bytebufferArray;

		/**
		 * Keeps the {@link Status} of each element in the
		 * {@link #bytebufferArray}
		 */
		private Map<Integer, AtomicReference<Status>> bufferStatus;

		public ByteBufferArrayOutputStream(int listSize) {
			debugPrint = 0;
			writeIndex = 0;
			readIndex = 0;
			bytebufferArray = new ByteBufferOutputStream[listSize];
			bufferStatus = new HashMap<>(listSize);
			for (int i = 0; i < bytebufferArray.length; i++) {
				bytebufferArray[i] = new ByteBufferOutputStream();
				bufferStatus.put(i,
						new AtomicReference<Status>(Status.canWrite));
			}
		}

		@Override
		public void write(int b) throws IOException {
			bytebufferArray[writeIndex].write(b);
		}

		public void write(byte b[], int off, int len) throws IOException {
			bytebufferArray[writeIndex].write(b, off, len);
		}

		/**
		 * Do not forget to call {@link #writeCompleted()} after every
		 * successful bulk writes. Whatever written in between these two calls
		 * will be captured into single {@link ByteBufferOutputStream}.
		 * 
		 * @return <code>true</code> iff the next buffer is free to write.
		 */
		public boolean newWrite() {
			if (bufferStatus.get(writeIndex).compareAndSet(Status.canWrite,
					Status.beingWritten)) {
				if (debugPrint > 0)
					System.out.println(Thread.currentThread().getName()
							+ " : newWrite-canWrite : " + "writeIndex - "
							+ writeIndex + ", readIndex - " + readIndex);
				return true;
			} else {
				if (debugPrint > 0)
					System.out.println(Thread.currentThread().getName()
							+ " : newWrite-failed : " + "writeIndex - "
							+ writeIndex + ", readIndex - " + readIndex);
				return false;
			}
		}

		/**
		 * Writer must call this method right after the writing of an collection
		 * of objects is completed.
		 */
		public void writeCompleted() {
			if (debugPrint > 0)
				System.out.println(Thread.currentThread().getName()
						+ " : writeCompleted : " + "writeIndex - " + writeIndex
						+ ", readIndex - " + readIndex);
			int w = writeIndex;
			writeIndex = (writeIndex + 1) % bytebufferArray.length;
			boolean ret = bufferStatus.get(w).compareAndSet(
					Status.beingWritten, Status.canRead);
			if (!ret)
				throw new IllegalStateException("bufferStatus conflict");
		}

		/**
		 * Do not forget to call {@link #readCompleted()} after every successful
		 * read of a {@link ByteBufferOutputStream}.
		 * 
		 * @return Next available {@link ByteBufferOutputStream} if available or
		 *         <code>null</code> if no {@link ByteBufferOutputStream} is
		 *         available to read.
		 */
		public synchronized ByteBufferOutputStream newRead() {
			if (bufferStatus.get(readIndex).get() == Status.beingRead) {
				if (debugPrint > 0)
					System.out.println(Thread.currentThread().getName()
							+ " : newRead-beingRead : " + "writeIndex - "
							+ writeIndex + ", readIndex - " + readIndex);
				return null;
			}

			if (bufferStatus.get(readIndex).compareAndSet(Status.canRead,
					Status.beingRead)) {
				if (debugPrint > 0)
					System.out.println(Thread.currentThread().getName()
							+ " : newRead-canRead : " + "writeIndex - "
							+ writeIndex + ", readIndex - " + readIndex);
				if (bytebufferArray[readIndex].getCount() == 0) {
					throw new IllegalStateException(
							"bytebufferArray[a].getCount() != 0 is expected.");
				}
				return bytebufferArray[readIndex];
			} else {
				if (debugPrint > 0)
					System.out.println(Thread.currentThread().getName()
							+ " : newRead - not can read " + readIndex);
				return null;
			}
		}

		/**
		 * Reader must call this method right after the reading process is
		 * completed.
		 */
		public void readCompleted() {
			if (debugPrint > 0)
				System.out.println(Thread.currentThread().getName()
						+ " : readCompleted : " + "writeIndex - " + writeIndex
						+ ", readIndex - " + readIndex);
			bytebufferArray[readIndex].reset();
			int r = readIndex;
			readIndex = (readIndex + 1) % bytebufferArray.length;
			boolean ret = bufferStatus.get(r).compareAndSet(Status.beingRead,
					Status.canWrite);
			if (!ret)
				throw new IllegalStateException("bufferStatus conflict");
		}
	}

	/**
	 * Uniquely identifies a Asynchronous TCP connection among all connected
	 * machines.
	 * 
	 * <p>
	 * NOTE: IPAddress is not included for the moment to avoid re-sending same
	 * information again and again for every reconfiguration. machineId to
	 * {@link NodeInfo} map will be sent initially. So {@link StreamNode}s can
	 * get ipAddress of a machine from that map.
	 */
	public static class AsyncTCPConnectionInfo extends ConnectionInfo {

		private static final long serialVersionUID = 1L;

		private final int portNo;

		public AsyncTCPConnectionInfo(int srcID, int dstID, int portNo) {
			super(srcID, dstID, false);
			Ipv4Validator validator = Ipv4Validator.getInstance();
			if (!validator.isValid(portNo))
				throw new IllegalArgumentException("Invalid port No");
			this.portNo = portNo;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + portNo;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (getClass() != obj.getClass())
				return false;
			AsyncTCPConnectionInfo other = (AsyncTCPConnectionInfo) obj;
			if (portNo != other.portNo)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "AsyncTCPConnectionInfo [srcID=" + getSrcID() + ", dstID="
					+ getDstID() + ", portID=" + portNo + "]";
		}

		@Override
		public Connection makeConnection(int nodeID, NetworkInfo networkInfo,
				int timeOut) {
			Connection con = null;
			if (srcID == nodeID) {
				try {
					con = ConnectionFactory.getAsyncConnection(portNo);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			else if (dstID == nodeID) {
				InetAddress ipAddress = networkInfo.getInetAddress(srcID);
				try {
					con = ConnectionFactory.getConnection(
							ipAddress.getHostAddress(), portNo, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				throw new IllegalArgumentException(
						"Neither srcID nor dstID matches with nodeID");
			}
			return con;
		}

		@Override
		public BoundaryInputChannel inputChannel(Token t, int bufSize,
				TCPConnectionProvider conProvider) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public BoundaryOutputChannel outputChannel(Token t, int bufSize,
				TCPConnectionProvider conProvider) {
			// TODO Auto-generated method stub
			return null;
		}
	}

	public static class AsyncTCPBuffer extends AbstractWriteOnlyBuffer {

		private final AsynchronousTCPConnection con;

		public AsyncTCPBuffer(AsynchronousTCPConnection con) {
			this.con = con;
		}

		@Override
		public boolean write(Object t) {
			try {
				con.writeObject(t);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}

		public int write(Object[] data, int offset, int length) {
			try {
				return con.write(data, offset, length);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
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
}
