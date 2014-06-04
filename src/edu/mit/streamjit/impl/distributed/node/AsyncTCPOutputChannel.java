package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection;
import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection.AsyncTCPBuffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;

public class AsyncTCPOutputChannel implements BoundaryOutputChannel {

	private Connection con;

	private final String name;

	private final TCPConnectionProvider conProvider;

	private final ConnectionInfo conInfo;

	private AsyncTCPBuffer buffer = null;

	private volatile boolean isFinal;

	private volatile boolean stopCalled;

	public AsyncTCPOutputChannel(TCPConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
		name = "AsyncTCPOutputChannel " + bufferTokenName;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
		isFinal = false;
		stopCalled = false;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (con == null || !con.isStillConnected()) {
					try {
						con = conProvider.getConnection(conInfo);
						buffer = new AsyncTCPBuffer(
								(AsynchronousTCPConnection) con);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
	}

	@Override
	public ImmutableList<Object> getUnprocessedData() {
		return null;
	}

	@Override
	public void stop(boolean isFinal) {
		this.isFinal = isFinal;
		if (!stopCalled) {
			try {
				con.softClose();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		stopCalled = true;
	}

	@Override
	public void sendData() {

	}

	@Override
	public Connection getConnection() {
		return con;
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return conInfo;
	}

	@Override
	public Buffer getBuffer() {
		return buffer;
	}
}
