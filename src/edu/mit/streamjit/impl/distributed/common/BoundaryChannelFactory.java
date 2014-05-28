package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.AsyncTCPOutputChannel;
import edu.mit.streamjit.impl.distributed.node.TCPInputChannel;
import edu.mit.streamjit.impl.distributed.node.TCPOutputChannel;

/**
 * {@link BoundaryChannel} maker.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2014
 */
public interface BoundaryChannelFactory {

	BoundaryInputChannel makeInputChannel(Token t, Buffer buffer,
			ConnectionInfo conInfo);

	BoundaryOutputChannel makeOutputChannel(Token t, Buffer buffer,
			ConnectionInfo conInfo);

	/**
	 * Makes blocking {@link TCPInputChannel} and {@link TCPOutputChannel}.
	 * 
	 */
	public static class TCPBoundaryChannelFactory
			implements
				BoundaryChannelFactory {

		protected final TCPConnectionProvider conProvider;

		public TCPBoundaryChannelFactory(TCPConnectionProvider conProvider) {
			this.conProvider = conProvider;
		}

		@Override
		public BoundaryInputChannel makeInputChannel(Token t, Buffer buffer,
				ConnectionInfo conInfo) {
			return new TCPInputChannel(buffer, conProvider, conInfo,
					t.toString(), 0);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, Buffer buffer,
				ConnectionInfo conInfo) {
			return new TCPOutputChannel(buffer, conProvider, conInfo,
					t.toString(), 0);
		}
	}

	/**
	 * Makes blocking {@link TCPInputChannel} and asynchronous
	 * {@link AsyncTCPOutputChannel}.
	 * 
	 */
	public class AsyncBoundaryChannelFactory extends TCPBoundaryChannelFactory {

		public AsyncBoundaryChannelFactory(TCPConnectionProvider conProvider) {
			super(conProvider);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, Buffer buffer,
				ConnectionInfo conInfo) {
			return new AsyncTCPOutputChannel(conProvider, conInfo,
					t.toString(), 0);
		}
	}
}