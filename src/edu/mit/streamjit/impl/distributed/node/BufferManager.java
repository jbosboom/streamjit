package edu.mit.streamjit.impl.distributed.node;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
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
	 * @return local buffers if buffer sizes are calculated. Otherwise returns
	 *         null.
	 */
	ImmutableMap<Token, Buffer> localBufferMap();

	public static abstract class AbstractBufferManager implements BufferManager {

		protected final Set<Blob> blobSet;

		protected final ImmutableSet<Token> localTokens;

		protected final ImmutableSet<Token> globalInputTokens;

		protected final ImmutableSet<Token> globalOutputTokens;

		protected boolean isbufferSizesReady;

		protected ImmutableMap<Token, Integer> bufferSizes;

		ImmutableMap<Token, Buffer> localBufferMap;

		public AbstractBufferManager(Set<Blob> blobSet) {
			this.blobSet = blobSet;

			Set<Token> inputTokens = new HashSet<>();
			Set<Token> outputTokens = new HashSet<>();
			for (Blob b : blobSet) {
				inputTokens.addAll(b.getInputs());
				outputTokens.addAll(b.getOutputs());
			}

			localTokens = ImmutableSet.copyOf(Sets.intersection(inputTokens,
					outputTokens));
			globalInputTokens = ImmutableSet.copyOf(Sets.difference(
					inputTokens, localTokens));
			globalOutputTokens = ImmutableSet.copyOf(Sets.difference(
					outputTokens, localTokens));

			isbufferSizesReady = false;
			bufferSizes = null;
			localBufferMap = null;
		}

		@Override
		public ImmutableSet<Token> localTokens() {
			return localTokens;
		}

		@Override
		public ImmutableSet<Token> outputTokens() {
			return globalOutputTokens;
		}

		@Override
		public ImmutableSet<Token> inputTokens() {
			return globalInputTokens;
		}

		@Override
		public ImmutableMap<Token, Integer> bufferSizes() {
			return bufferSizes;
		}

		@Override
		public boolean isbufferSizesReady() {
			return isbufferSizesReady;
		}

		@Override
		public ImmutableMap<Token, Buffer> localBufferMap() {
			return localBufferMap;
		}
	}
}
