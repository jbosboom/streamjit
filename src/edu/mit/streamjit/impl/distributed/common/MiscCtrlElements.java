package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;

public abstract class MiscCtrlElements implements CTRLRMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(MiscCtrlElementProcessor miscProcessor);

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public static final class NewConInfo extends MiscCtrlElements {
		private static final long serialVersionUID = 1L;

		public final TCPConnectionInfo conInfo;
		public final Token token;

		public NewConInfo(TCPConnectionInfo conInfo, Token token) {
			this.conInfo = conInfo;
			this.token = token;
		}

		@Override
		public void process(MiscCtrlElementProcessor miscProcessor) {
			miscProcessor.process(this);
		}
	}

	public interface MiscCtrlElementProcessor {

		public void process(NewConInfo newConInfo);
	}
}