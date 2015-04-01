package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.distributed.node.LocalBuffer.ConcurrentArrayLocalBuffer;
import edu.mit.streamjit.impl.distributed.node.LocalBuffer.LocalBuffer1;

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
	ImmutableMap<Token, LocalBuffer> localBufferMap();

	public static abstract class AbstractBufferManager implements BufferManager {

		protected final Set<Blob> blobSet;

		protected final ImmutableSet<Token> localTokens;

		protected final ImmutableSet<Token> globalInputTokens;

		protected final ImmutableSet<Token> globalOutputTokens;

		protected boolean isbufferSizesReady;

		protected ImmutableMap<Token, Integer> bufferSizes;

		ImmutableMap<Token, LocalBuffer> localBufferMap;

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
		public ImmutableMap<Token, LocalBuffer> localBufferMap() {
			return localBufferMap;
		}

		protected final void createLocalBuffers() {
			ImmutableMap.Builder<Token, LocalBuffer> bufferMapBuilder = ImmutableMap
					.<Token, LocalBuffer> builder();
			for (Token t : localTokens) {
				int bufSize = bufferSizes.get(t);
				bufferMapBuilder
						.put(t, concurrentArrayLocalBuffer1(t, bufSize));
			}
			localBufferMap = bufferMapBuilder.build();
		}

		protected final LocalBuffer1 concurrentArrayLocalBuffer1(Token t,
				int bufSize) {
			List<Object> args = new ArrayList<>(1);
			args.add(bufSize);
			return new LocalBuffer1(t.toString(), ConcurrentArrayBuffer.class,
					args, bufSize, 0);
		}

		protected final LocalBuffer concurrentArrayLocalBuffer(Token t,
				int bufSize) {
			return new ConcurrentArrayLocalBuffer(bufSize);
		}

		/**
		 * Just introduced to avoid code duplication.
		 */
		protected void addBufferSize(Token t, int minSize,
				ImmutableMap.Builder<Token, Integer> bufferSizeMapBuilder) {
			// TODO: Just to increase the performance. Change it later
			int bufSize = Math.max(1000, minSize);
			// System.out.println("Buffer size of " + t.toString() + " is " +
			// bufSize);
			bufferSizeMapBuilder.put(t, bufSize);
		}
	}

	/**
	 * Calculates buffer sizes locally at {@link StreamNode} side. No central
	 * calculation involved.
	 */
	public static class SNLocalBufferManager extends AbstractBufferManager {
		public SNLocalBufferManager(Set<Blob> blobSet) {
			super(blobSet);
		}

		@Override
		public void initialise() {
			bufferSizes = calculateBufferSizes(blobSet);
			createLocalBuffers();
			isbufferSizesReady = true;
		}

		@Override
		public void initialise2(Map<Token, Integer> minInputBufSizes) {
			throw new java.lang.Error(
					"initialise2() is not supposed to be called");
		}

		// TODO: Buffer sizes, including head and tail buffers, must be
		// optimised. consider adding some tuning factor
		private ImmutableMap<Token, Integer> calculateBufferSizes(
				Set<Blob> blobSet) {
			ImmutableMap.Builder<Token, Integer> bufferSizeMapBuilder = ImmutableMap
					.<Token, Integer> builder();

			Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
			Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

			for (Blob b : blobSet) {
				Set<Blob.Token> inputs = b.getInputs();
				for (Token t : inputs) {
					minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
				}

				Set<Blob.Token> outputs = b.getOutputs();
				for (Token t : outputs) {
					minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
				}
			}

			Set<Token> localTokens = Sets.intersection(
					minInputBufCapaciy.keySet(), minOutputBufCapaciy.keySet());
			Set<Token> globalInputTokens = Sets.difference(
					minInputBufCapaciy.keySet(), localTokens);
			Set<Token> globalOutputTokens = Sets.difference(
					minOutputBufCapaciy.keySet(), localTokens);

			for (Token t : localTokens) {
				int bufSize = Math.max(minInputBufCapaciy.get(t),
						minOutputBufCapaciy.get(t));
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}

			for (Token t : globalInputTokens) {
				int bufSize = minInputBufCapaciy.get(t);
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}

			for (Token t : globalOutputTokens) {
				int bufSize = minOutputBufCapaciy.get(t);
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}
			return bufferSizeMapBuilder.build();
		}
	}
}
