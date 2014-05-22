package edu.mit.streamjit.impl.blob;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection;

public class AsyncTCPBuffer extends AbstractWriteOnlyBuffer {

	private final AsynchronousTCPConnection con;

	public AsyncTCPBuffer(AsynchronousTCPConnection con) {
		this.con = con;
	}

	@Override
	public boolean write(Object t) {
		try {
			con.writeObject(t);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public int write(Object[] data, int offset, int length) {
		try {
			return con.write(data, offset, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public int capacity() {
		return Integer.MAX_VALUE;
	}
}
