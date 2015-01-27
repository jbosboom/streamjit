package edu.mit.streamjit.impl.distributed.profiler;

import java.io.Serializable;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageVisitor;
import edu.mit.streamjit.impl.distributed.node.BlobsManager;

public abstract class SNProfileElement implements SNMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(SNProfileElementProcessor dp);

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * Status for all buffers from a {@link BlobsManager}.
	 */
	public static final class SNBufferStatusData extends SNProfileElement {

		private static final long serialVersionUID = 1L;

		public final int machineID;

		public final ImmutableSet<BlobBufferStatus> blobsBufferStatusSet;

		public SNBufferStatusData(int machineID,
				ImmutableSet<BlobBufferStatus> blobsBufferStatusSet) {
			this.machineID = machineID;
			this.blobsBufferStatusSet = blobsBufferStatusSet;
		}

		/**
		 * Status of all buffers of a blob.
		 */
		public static class BlobBufferStatus implements Serializable {

			private static final long serialVersionUID = 1L;

			/**
			 * Identifier of the blob. blobID can be get through
			 * Utils#getBlobID().
			 */
			public final Token blobID;

			/**
			 * BufferStatus of all input channels of the blob.
			 */
			public final ImmutableSet<BufferStatus> inputSet;

			/**
			 * BufferStatus of all output channels of the blob.
			 */
			public final ImmutableSet<BufferStatus> outputSet;

			public BlobBufferStatus(Token blobID,
					ImmutableSet<BufferStatus> inputSet,
					ImmutableSet<BufferStatus> outputSet) {
				this.blobID = blobID;
				this.inputSet = inputSet;
				this.outputSet = outputSet;
			}

			@Override
			public String toString() {
				return String.format("BlobBufferStatus:blob=%s", blobID);
			}
		}

		/**
		 * Status of a single buffer.
		 */
		public static class BufferStatus implements Serializable {

			private static final long serialVersionUID = 1L;

			/**
			 * Token of the buffer.
			 */
			public final Token ID;

			/**
			 * Minimum buffer requirement. Blob.getMinimumBufferCapacity() gives
			 * this information.
			 */
			public final int min;

			/**
			 * Available resources in the buffer. If it is a input buffer then
			 * buffer.size(). If it is a output buffer then buffer.capacity() -
			 * buffer.size().
			 */
			public final int availableResource;

			public BufferStatus(Token ID, int min, int availableResource) {
				this.ID = ID;
				this.min = min;
				this.availableResource = availableResource;
			}

			@Override
			public String toString() {
				return String.format("Buffer=%s, min=%d, available=%d", ID,
						min, availableResource);
			}
		}

		@Override
		public void process(SNProfileElementProcessor dp) {
			dp.process(this);
		}
	}

	public interface SNProfileElementProcessor {
		public void process(SNBufferStatusData bufferStatusData);
	}
}
