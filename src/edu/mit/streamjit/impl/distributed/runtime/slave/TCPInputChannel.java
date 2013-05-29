package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.IOException;
import java.util.List;

import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.runtime.common.TCPSocket;
import edu.mit.streamjit.impl.distributed.runtime.master.ListenerSocket;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * TCPInputChannel acts as client when making TCP connection. 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel<E> implements BoundaryInputChannel<E> {
	
	Channel<E> channel;
	
	TCPSocket socket;

	int portNo;
	String IpAddress;
	
	TCPInputChannel( Channel<E> channel, int portNo)
	{
		this.channel = channel;
		this.portNo = portNo;
	}

	@Override
	public void closeConnection() throws IOException {
		socket.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void makeConnection() throws IOException {
		
	}

	@Override
	public Runnable getRunnable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getOtherMachineID() {
		// TODO Auto-generated method stub
		return 0;
	}

}
