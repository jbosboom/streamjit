package edu.mit.streamjit.impl.interp;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.impl.blob.Buffer;

/**
 * A skeletal implementation of CompiledStream that forwards offer() and poll()
 * to write() and read() operations on Buffer instances.
 *
 * This class provides a properly-synchronized implementation of drain() and
 * related methods using volatile variables and a CountDownLatch.  offer() and
 * poll() are not synchronized; if the implementation requires interacting with
 * other threads, synchronized Buffer implementations should be used.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/3/2013
 */
public abstract class AbstractCompiledStream<I, O> implements CompiledStream<I, O> {
	/**
	 * The buffer being used for input: offer() turns into write().
	 */
	private final Buffer inputBuffer;
	/**
	 * The buffer being used for output: poll() turns into read().
	 */
	private final Buffer outputBuffer;
	/**
	 * Whether the stream is in the process of draining (or already drained);
	 * specifically, whether calls to offer() should be forwarded to the channel
	 * or not.
	 */
	private volatile boolean draining = false, drainingComplete = false;
	/**
	 * Once the awaitDrainingLatch is cleared, indicates whether the stream was
	 * fully drained or if there were data items stuck in buffers.
	 */
	private volatile boolean fullyDrained = false;

	/**
	 * Creates a new AbstractCompiledStream, using the given buffers for input
	 * and output.  If the implementation does anything with threads, these
	 * buffers should be synchronized.
	 * @param inputBuffer the buffer to use for input
	 * @param outputBuffer the buffer to use for output
	 */
	public AbstractCompiledStream(Buffer inputBuffer, Buffer outputBuffer) {
		if (inputBuffer == null || outputBuffer == null)
			throw new NullPointerException();
		this.inputBuffer = inputBuffer;
		this.outputBuffer = outputBuffer;
	}

	@Override
	public boolean offer(I input) {
		if (draining)
			return false;
		return inputBuffer.write(input);
	}

	@Override
	@SuppressWarnings("unchecked")
	public O poll() {
		return (O)outputBuffer.read();
	}

	@Override
	public final void drain() {
		draining = true;
		doDrain();
	}

	@Override
	public boolean isDrained() {
		return drainingComplete;
	}

	/**
	 * Called when a client calls drain() on this stream, to be implemented by
	 * subclasses to begin draining and resource reclamation.  Note that this
	 * method is called on the thread that called drain(), which should not
	 * block; if interactions with other threads are required, the
	 * implementation should set a flag or otherwise communicate the drain
	 * request to another thread.  finishedDraining() should be called when
	 * draining is finished.
	 */
	protected abstract void doDrain();

	/**
	 * Called by an implementation when draining is complete, to release clients
	 * blocked in awaitDraining().  This method should only be called once.
	 * @param fullyDrained true if the stream was fully drained, false if data
	 * items remained in buffers
	 */
	protected final void finishedDraining(boolean fullyDrained) {
		//TODO: enforce that the method is called once?
		this.fullyDrained = fullyDrained;
		this.drainingComplete = true;
	}
}
