package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.api.BlobsManager;
import edu.mit.streamjit.impl.distributed.api.Command;
import edu.mit.streamjit.impl.distributed.api.MessageElement;
import edu.mit.streamjit.impl.distributed.api.MessageVisitor;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Ipv4Validator;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * In StreamJit's jargon "Stream node" means a computing node that runs part or full a streamJit application. </p> Here, the class
 * {@link StreamNode} is a StreamJit's run timer for each distributed node. So there can be only one {@link StreamNode} instance per
 * computing node. Once it got connected with the {@link Controller}, it will keep on listening and processing the commands from the
 * {@link Controller}. {@link Controller} can issue the {@link Command} EXIT to stop the streamNode.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
public class StreamNode extends Thread {

	/**
	 * Lets keep the package public (default) for the moment.
	 */
	Connection controllerConnection;
	private int machineID; // TODO: consider move or remove this from StreamNode class. If so, this class will be more handy.
	private MessageVisitor mv;

	private BlobsManager blobsManager;

	private boolean run; // As we assume that all controller communication and the MessageElement processing is managed by single
							// thread,
							// no need to make this variable thread safe.

	public void exit() {
		this.run = false;
	}

	/**
	 * Only IP address is required. PortNo is optional. If it is not provided, {@link StreamNode} will try to start with default
	 * StreamJit's port number that can be found {@link GlobalConstants}.
	 */
	public StreamNode(Connection connection) {
		this.controllerConnection = connection;
		this.mv = new NodeMessageVisitor(new AppStatusProcessorImpl(), new CommandProcessorImpl(this), new ErrorProcessorImpl(),
				new RequestProcessorImpl(this), new JsonStringProcessorImpl(this));
		this.run = true;
	}

	public void run() {
		while (run) {
			try {
				MessageElement me = controllerConnection.readObject();
				me.accept(mv);
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
				// TODO: Need to decide what to do when exception occurred.
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

	public int getMachineID() {
		return machineID;
	}

	public void setMachineID(int machineID) {
		this.machineID = machineID;
		System.out.println("I have got my machine ID: " + this.machineID);
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

	public String tostString() {
		return "StreamNode-" + machineID;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int requiredArgCount = 1; // Port no is optional.
		String ipAddress;

		if (args.length < requiredArgCount) {
			/*
			 * System.out.println(args.length);
			 * System.out.println("Not enough parameters passed. Please provide thr following parameters.");
			 * System.out.println("0: Controller's IP address"); System.exit(0);
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
			System.out.println("Creating connection with the controller failed.");
		}
	}
}
