package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;

public class AsyncTCPOutputChannel implements BoundaryOutputChannel {

	private Connection con;

	private final String name;

	private final TCPConnectionProvider conProvider;

	TCPConnectionInfo conInfo;

	public AsyncTCPOutputChannel(TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
		name = "AsyncTCPOutputChannel " + bufferTokenName;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public final void closeConnection() throws IOException {
		con.softClose();
	}

	@Override
	public boolean isStillConnected() {
		return (con == null) ? false : con.isStillConnected();
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (con == null || !con.isStillConnected()) {
					try {
						con = conProvider.getConnection(conInfo);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
	}

	@Override
	public int getOtherNodeID() {
		return 0;
	}

	@Override
	public ImmutableList<Object> getUnprocessedData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop(boolean isFinal) {

	}

	@Override
	public void sendData() {

	}

	public AsynchronousTCPConnection getConnection() {
		return (AsynchronousTCPConnection) con;
	}
}
