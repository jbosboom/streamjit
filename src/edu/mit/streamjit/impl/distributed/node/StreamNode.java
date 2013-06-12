/**
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.api.BlobsManager;
import edu.mit.streamjit.impl.distributed.api.Command;
import edu.mit.streamjit.impl.distributed.api.MessageElement;
import edu.mit.streamjit.impl.distributed.api.MessageVisitor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Ipv4Validator;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * This class is driving class at streamNode side. Once it is started, it will keep on listening and processing the commands from the
 * {@link Controller}. {@link Controller} can issue the {@link Command} EXIT to stop the streamNode.
 */
public class StreamNode {

	Connection controllerConnection;
	private int machineID; // TODO: consider move or remove this from StreamNode class. If so, this class will be more handy.
	MessageVisitor mv;

	BlobsManager blobsManager;

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
	public StreamNode(String ipAddress, int portNo) {
		controllerConnection = new TCPNodeConnection(ipAddress, portNo);
		this.mv = new NodeMessageVisitor(new AppStatusProcessorImpl(), new CommandProcessorImpl(this), new ErrorProcessorImpl(),
				new RequestProcessorImpl(this), new JsonStringProcessorImpl(this));
		this.run = true;
	}

	/**
	 * Only IP address is required. PortNo is optional. If it is not provided, {@link StreamNode} will try to start with default
	 * StreamJit's port number that can be found {@link GlobalConstants}.
	 */
	public StreamNode(String ipAddress) {
		this(ipAddress, GlobalConstants.PORTNO);
	}

	public void run() {

		try {
			controllerConnection.makeConnection();
		} catch (IOException e1) {
			System.out.println("Couldn't extablish the connection with Controller node. I am terminating...");
			e1.printStackTrace();
			System.exit(0);
		}

		while (run) {
			try {
				MessageElement me = controllerConnection.readObject();
				me.accept(mv);
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
		releaseResources();
	}

	public int getMachineID() {
		return machineID;
	}

	public void setMachineID(int machineID) {
		this.machineID = machineID;
		System.out.println("I have got my machine ID: " + this.machineID);
	}

	// Release all file pointers, opened sockets, etc.
	private void releaseResources() {
		try {
			this.controllerConnection.closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

		if (args.length > 1) {
			int portNo;
			try {
				portNo = Integer.parseInt(args[1]);
				new StreamNode(ipAddress, portNo).run();

			} catch (NumberFormatException ex) {
				System.out.println("Invalid port No...");
				System.out.println("Please verify the second argument.");
				System.exit(0);
			}
		} else
			new StreamNode(ipAddress).run();
	}
}
