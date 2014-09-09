package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.AsyncOutputChannel;
import edu.mit.streamjit.impl.distributed.node.BlockingInputChannel;
import edu.mit.streamjit.impl.distributed.node.BlockingOutputChannel;

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

	BoundaryInputChannel makeInputChannel(Token t, int bufSize,
			ConnectionInfo conInfo);

	BoundaryOutputChannel makeOutputChannel(Token t, int bufSize,
			ConnectionInfo conInfo);

	/**
	 * Makes blocking {@link BlockingInputChannel} and {@link BlockingOutputChannel}.
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
			return new BlockingInputChannel(buffer, conProvider, conInfo,
					t.toString(), 0);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, Buffer buffer,
				ConnectionInfo conInfo) {
			return new BlockingOutputChannel(buffer, conProvider, conInfo,
					t.toString(), 0);
		}

		@Override
		public BoundaryInputChannel makeInputChannel(Token t, int bufSize,
				ConnectionInfo conInfo) {
			return new BlockingInputChannel(bufSize, conProvider, conInfo,
					t.toString(), 0);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, int bufSize,
				ConnectionInfo conInfo) {
			return new BlockingOutputChannel(bufSize, conProvider, conInfo,
					t.toString(), 0);
		}
	}

	/**
	 * Makes blocking {@link BlockingInputChannel} and asynchronous
	 * {@link AsyncOutputChannel}.
	 * 
	 */
	public class AsyncBoundaryChannelFactory extends TCPBoundaryChannelFactory {

		public AsyncBoundaryChannelFactory(TCPConnectionProvider conProvider) {
			super(conProvider);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, Buffer buffer,
				ConnectionInfo conInfo) {
			return new AsyncOutputChannel(conProvider, conInfo,
					t.toString(), 0);
		}

		@Override
		public BoundaryOutputChannel makeOutputChannel(Token t, int bufSize,
				ConnectionInfo conInfo) {
			return new AsyncOutputChannel(conProvider, conInfo,
					t.toString(), 0);
		}
	}
}