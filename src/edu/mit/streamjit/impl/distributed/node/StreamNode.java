package edu.mit.streamjit.impl.distributed.node;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;

import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Ipv4Validator;
import edu.mit.streamjit.impl.distributed.profiler.Profiler;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * In StreamJit's jargon "Stream node" means a computing node that runs part or
 * full a streamJit application. </p> Here, the class StreamNode is a
 * StreamJit's run timer for each distributed node. So StreamNode is singleton
 * pattern as there can be only one StreamNode instance per computing node. Once
 * it got connected with the {@link Controller}, it will keep on listening and
 * processing the commands from the Controller. Controller can issue the
 * {@link Command} EXIT to stop the streamNode.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
public class StreamNode extends Thread {

	/**
	 * Lets keep this as package public (default) for the moment.
	 */
	Connection controllerConnection;
	private int myNodeID = -1; // TODO: consider move or remove this from
								// StreamNode class. If so, this class will be
								// more handy.
	private CTRLRMessageVisitor mv;

	private volatile BlobsManager blobsManager;

	Profiler profiler;

	private boolean run; // As we assume that all controller communication and
							// the MessageElement processing is managed by
							// single
							// thread,
							// no need to make this variable thread safe.

	private static StreamNode myinstance;

	/**
	 * Thread safe way of Singleton pattern.
	 */
	public static StreamNode getInstance(Connection connection) {
		if (myinstance == null) {
			synchronized (StreamNode.class) {
				if (myinstance == null)
					myinstance = new StreamNode(connection);
			}
		}
		return myinstance;
	}

	/**
	 * {@link StreamNode} is Singleton pattern in order to ensure one instance
	 * per JVM..
	 */
	private StreamNode(Connection connection) {
		super("Stream Node");
		this.controllerConnection = connection;
		this.mv = new CTRLRMessageVisitorImpl(this);
		this.run = true;
	}

	public void run() {
		System.out.println("Connected with Controller.");
		while (run) {
			try {
				CTRLRMessageElement me = controllerConnection.readObject();
				me.accept(mv);
			} catch (ClassNotFoundException e) {
				// No way. Just ignore.
			} catch (EOFException e) {
				// Other side closed
				run = false;
			} catch (IOException e) {
				e.printStackTrace();
				// TODO: Need to decide what to do here. May be we can re try
				// couple of time in a time interval before aborting the
				// execution.
				run = false;
			}
		}
		safelyCloseResources();
	}

	public int getNodeID() {
		return myNodeID;
	}

	public void setNodeID(int nodeID) {
		this.myNodeID = nodeID;
		System.out.println("I have got my node ID: " + this.myNodeID);
		Thread.currentThread().setName(this.tostString());
	}

	/**
	 * @return the blobsManager
	 */
	public BlobsManager getBlobsManager() {
		return blobsManager;
	}

	/**
	 * @param blobsManager
	 *            the blobsManager to set
	 */
	public void setBlobsManager(BlobsManager blobsManager) {
		releaseOldBM();
		this.blobsManager = blobsManager;
		if (profiler != null && blobsManager != null)
			profiler.addAll(blobsManager.profilers());
	}

	public void exit() {
		this.run = false;
	}

	public String tostString() {
		return "StreamNode-" + myNodeID;
	}

	private void safelyCloseResources() {
		if (blobsManager != null)
			blobsManager.stop();

		if (profiler != null)
			profiler.stopProfiling();

		try {
			this.controllerConnection.closeConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Un-references old BlobManager object before creating new one.
	 */
	void releaseOldBM() {
		// [2014-3-20] We need to release blobsmanager to release the
		// memory. Otherwise, Blobthread2.corecode will occupy the memory.
		if (blobsManager != null) {
			blobsManager.stop();
			if (profiler != null)
				profiler.removeAll(blobsManager.profilers());
			blobsManager = null;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String ipAddress;
		int portNo = 0;

		switch (args.length) {
			case 1 :
				ipAddress = args[0];
				portNo = GlobalConstants.PORTNO;
				break;

			case 2 :
				ipAddress = args[0];
				try {
					portNo = Integer.parseInt(args[1]);
				} catch (NumberFormatException ex) {
					System.err.println("Invalid port No...");
					System.err.println("Please verify the second argument.");
					System.exit(0);
				}
				break;
			default :
				ipAddress = "127.0.0.1";
				portNo = GlobalConstants.PORTNO;
		}

		if (!Ipv4Validator.getInstance().isValid(ipAddress)) {
			System.err.println("Invalid IP address...");
			System.err.println("Please verify the first argument.");

			System.exit(0);
		}

		if (portNo < 1024) {
			System.err
					.println("Wellknown port number has been used. Please consider avoid using it");
		} else if (portNo > 65535) {
			System.err.println("Invalid port no...");
			System.exit(0);
		}

		Connection tcpConnection;
		try {
			tcpConnection = ConnectionFactory.getConnection(ipAddress, portNo,
					true);
			new StreamNode(tcpConnection).run();
		} catch (ConnectException cex) {
			System.out.println("No Controller is listening. Terminating.");
		} catch (IOException e) {
			e.printStackTrace();
			System.out
					.println("Creating connection with the controller failed.");
		}
	}
}
