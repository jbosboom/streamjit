package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;

/**
 * {@link CommandProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class CommandProcessorImpl implements CommandProcessor {
	StreamNode streamNode;

	public CommandProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void processSTART() {
		BlobsManager bm = streamNode.getBlobsManager();
		if (bm != null) {
			bm.start();
			long heapMaxSize = Runtime.getRuntime().maxMemory();
			long heapSize = Runtime.getRuntime().totalMemory();
			long heapFreeSize = Runtime.getRuntime().freeMemory();

			System.out
					.println("##############################################");

			System.out.println("heapMaxSize = " + heapMaxSize / 1e6);
			System.out.println("heapSize = " + heapSize / 1e6);
			System.out.println("heapFreeSize = " + heapFreeSize / 1e6);
			System.out.println("StraemJit app is running...");
			System.out
					.println("##############################################");

		} else {
			// TODO: Need to handle this case. Need to send the error message to
			// the controller.
			System.out
					.println("Couldn't start the blobs...BlobsManager is null.");
		}
	}

	@Override
	public void processSTOP() {
		BlobsManager bm = streamNode.getBlobsManager();
		if (bm != null) {
			bm.stop();
			System.out.println("StraemJit app stopped...");
			try {
				streamNode.controllerConnection.writeObject(AppStatus.STOPPED);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// TODO: Need to handle this case. Need to send the error message to
			// the controller.
			System.out
					.println("Couldn't stop the blobs...BlobsManager is null.");
		}
	}

	@Override
	public void processEXIT() {
		System.out.println("StreamNode is Exiting...");
		streamNode.exit();
	}
}
