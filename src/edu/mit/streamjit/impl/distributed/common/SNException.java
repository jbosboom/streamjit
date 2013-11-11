package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;

public class SNException implements SNMessageElement {

	private static final long serialVersionUID = 1L;

	public void process(SNExceptionProcessor exP) {
		exP.process(this);
	}

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public static final class AddressBindException extends SNException {
		private static final long serialVersionUID = 1L;

		public final TCPConnectionInfo conInfo;

		public AddressBindException(TCPConnectionInfo conInfo) {
			this.conInfo = conInfo;
		}
	}

	public interface SNExceptionProcessor {

		public void process(SNException ex);

		public void process(AddressBindException abEx);
	}
}
