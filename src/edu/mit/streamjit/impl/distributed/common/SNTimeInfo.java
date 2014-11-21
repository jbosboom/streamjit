package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * {@link StreamNode}s shall send the timing information such as compilation
 * time of each blob, draining time, draindata collection time, Init schedule
 * time and etc by sending {@link SNTimeInfo}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Nov 20, 2014
 * 
 */
public abstract class SNTimeInfo implements SNMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(SNTimeInfoProcessor snTimeInfoProcessor);

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public static final class CompilationTime extends SNTimeInfo {

		private static final long serialVersionUID = 1L;

		public final Token blobID;

		public final double milliSec;

		public CompilationTime(Token blobID, double milliSec) {
			this.blobID = blobID;
			this.milliSec = milliSec;
		}

		@Override
		public void process(SNTimeInfoProcessor snTimeInfoProcessor) {
			snTimeInfoProcessor.process(this);
		}

	}

	public interface SNTimeInfoProcessor {
		public void process(CompilationTime compilationTime);
	}

	public static class SNTimeInfoProcessorImpl implements SNTimeInfoProcessor {

		private final OutputStreamWriter osw;

		public SNTimeInfoProcessorImpl(OutputStream outputStream) {
			this(new OutputStreamWriter(outputStream));
		}

		public SNTimeInfoProcessorImpl() {
			this(System.out);
		}

		public SNTimeInfoProcessorImpl(OutputStreamWriter osw) {
			this.osw = osw;
		}

		@Override
		public void process(CompilationTime compilationTime) {
			String msg = String.format("Compilation time: Blob-%s-%.0fms\n",
					compilationTime.blobID, compilationTime.milliSec);
			try {
				osw.write(msg);
				osw.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
