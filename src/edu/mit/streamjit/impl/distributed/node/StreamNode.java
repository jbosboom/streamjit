package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Ipv4Validator;
import edu.mit.streamjit.impl.distributed.common.MessageElement;
import edu.mit.streamjit.impl.distributed.common.MessageVisitor;
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
	private MessageVisitor mv;

	private BlobsManager blobsManager;

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
		this.mv = new NodeMessageVisitor(new AppStatusProcessorImpl(),
				new CommandProcessorImpl(this), new ErrorProcessorImpl(),
				new RequestProcessorImpl(this), new JsonStringProcessorImpl(
						this), new DrainProcessorImpl(this));
		this.run = true;
	}

	public void run() {
		while (run) {
			try {
				MessageElement me = controllerConnection.readObject();
				me.accept(mv);
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
				// TODO: Need to decide what to do here. May be we can re try
				// couple of time in a time interval before aborting the
				// execution.
				run = false;
			}
		}

		try {
			this.controllerConnection.closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		this.blobsManager = blobsManager;
	}

	public void exit() {
		this.run = false;
	}

	public String tostString() {
		return "StreamNode-" + myNodeID;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int requiredArgCount = 1; // Port no is optional.
		String ipAddress;

		if (args.length < requiredArgCount) {
			/*
			 * System.out.println(args.length); System.out.println(
			 * "Not enough parameters passed. Please provide thr following parameters."
			 * ); System.out.println("0: Controller's IP address");
			 * System.exit(0);
			 */
		}

		// ipAddress = args[0];
		ipAddress = "127.0.0.1";
		if (!Ipv4Validator.getInstance().isValid(ipAddress)) {
			System.out.println("Invalid IP address...");
			System.out.println("Please verify the first argument.");

			System.exit(0);
		}

		int portNo = 0;
		if (args.length > 1) {
			try {
				portNo = Integer.parseInt(args[1]);
			} catch (NumberFormatException ex) {
				System.out.println("Invalid port No...");
				System.out.println("Please verify the second argument.");
				System.exit(0);
			}
		} else {
			portNo = GlobalConstants.PORTNO;
		}

		ConnectionFactory cf = new ConnectionFactory();
		Connection tcpConnection;
		try {
			tcpConnection = cf.getConnection(ipAddress, portNo);
			new StreamNode(tcpConnection).run();
		} catch (IOException e) {
			e.printStackTrace();
			System.out
					.println("Creating connection with the controller failed.");
		}
	}
}
