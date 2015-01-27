package edu.mit.streamjit.impl.distributed.profiler;

import com.google.common.collect.ImmutableMap;

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

		public final Token blobID;

		public final ImmutableMap<Token, Integer> inputBufferSizes;

		public final ImmutableMap<Token, Integer> outputBufferSizes;

		public SNBufferStatusData(Token blobID,
				ImmutableMap<Token, Integer> inputBufferSizes,
				ImmutableMap<Token, Integer> outputBufferSizes) {
			this.blobID = blobID;
			this.inputBufferSizes = inputBufferSizes;
			this.outputBufferSizes = outputBufferSizes;
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
