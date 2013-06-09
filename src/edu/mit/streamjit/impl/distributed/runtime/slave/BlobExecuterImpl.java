/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.util.ArrayList;
import java.util.List;

import com.sun.org.glassfish.external.statistics.BoundaryStatistic;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobExecuter;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryOutputChannel;
import edu.mit.streamjit.impl.interp.Channel;

public class BlobExecuterImpl implements BlobExecuter {

	Blob blob;
	List<Thread> blobThreads;

	List<BoundaryInputChannel<?>> inputChannels;
	List<BoundaryOutputChannel<?>> outputChannels;

	public BlobExecuterImpl(Blob blob) {
		this.blob = blob;
		this.blobThreads = new ArrayList<>();
		this.inputChannels = new ArrayList<>();
		this.outputChannels = new ArrayList<>();

		initialize();
	}

	private void initialize() {
		for (int i = 0; i < blob.getCoreCount(); i++) {
			blobThreads.add(new Thread(blob.getCoreCode(i)));
		}

		// TODO: Need to pass the machine info.
		int portNo = 23569;
		for (Channel<?> chnl : blob.getInputChannels().values()) {
			inputChannels.add(new TCPInputChannel<>(chnl, "127.0.0.1", portNo++));
		}

		for (Channel<?> chnl : blob.getOutputChannels().values()) {
			outputChannels.add(new TCPOutputChannel<>(chnl, portNo++));
		}
	}

	@Override
	public void start() {

		for (BoundaryChannel<?> bc : inputChannels) {
			new Thread(bc.getRunnable()).start();
		}

		for (BoundaryChannel<?> bc : outputChannels) {
			new Thread(bc.getRunnable()).start();
		}

		for (Thread t : blobThreads)
			t.start();
	}

	@Override
	public void stop() {

		for (BoundaryChannel<?> bc : inputChannels) {
			bc.stop();
		}

		for (BoundaryChannel<?> bc : outputChannels) {
			bc.stop();
		}

		for (Thread t : blobThreads)
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	@Override
	public void suspend() {
	}

	@Override
	public void resume() {
	}
}
