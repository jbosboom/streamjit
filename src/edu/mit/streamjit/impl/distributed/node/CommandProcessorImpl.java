package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;

/**
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
			System.out.println("StraemJit app started...");
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
	public void processDRAIN() {
		throw new IllegalArgumentException("Suspend feature is not supported");
	}

	@Override
	public void processEXIT() {
		System.out.println("StreamNode is Exiting...");
		streamNode.exit();
	}
}
