package edu.mit.streamjit.impl.distributed.node;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * {@link BlobsManager} will use the services from {@link BufferManager}.
 * Implementation of this interface is expected to do two tasks
 * <ol>
 * <li>Calculates buffer sizes.
 * <li>Create local buffers.
 * </ol>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2014
 * 
 */
public interface BufferManager {

	void initialise();

	/**
	 * Second initialisation. If the buffer sizes are computed by controller and
	 * send back to the {@link StreamNode}s, this method can be called with the
	 * minimum input buffer size requirement.
	 * 
	 * @param minInputBufSizes
	 */
	void initialise2(Map<Token, Integer> minInputBufSizes);

	ImmutableSet<Token> localTokens();

	ImmutableSet<Token> outputTokens();

	ImmutableSet<Token> inputTokens();

	/**
	 * @return buffer sizes of each local and boundary channels. Returns
	 *         <code>null</code> if the buffer sizes are not calculated yet.
	 *         {@link #isbufferSizesReady()} tells whether the buffer sizes are
	 *         calculated or not.
	 */
	ImmutableMap<Token, Integer> bufferSizes();

	/**
	 * @return <code>true</code> iff buffer sizes are calculated.
	 */
	boolean isbufferSizesReady();

	/**
	 * @return local buffers
	 */
	ImmutableMap<Token, Buffer> localBufferMap();
}
