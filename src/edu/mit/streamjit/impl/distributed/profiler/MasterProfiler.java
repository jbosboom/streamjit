package edu.mit.streamjit.impl.distributed.profiler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BlobBufferStatus;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData.BufferStatus;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNProfileElementProcessor;

/**
 * Profiling data from all StreamNodes come to this central point.
 * 
 * @author sumanan
 * @since 27 Jan, 2015
 */
public class MasterProfiler implements SNProfileElementProcessor {

	private final Map<Integer, SNBufferStatusData> BufferStatusDataMap;

	private Object lock = new Object();

	public MasterProfiler() {
		BufferStatusDataMap = new ConcurrentHashMap<>();
	}

	@Override
	public void process(SNBufferStatusData bufferStatusData) {
		BufferStatusDataMap.put(bufferStatusData.machineID, bufferStatusData);
		print(bufferStatusData);
	}

	private void print(SNBufferStatusData bufferStatusData) {
		synchronized (lock) {
			System.out.println("MachineID=" + bufferStatusData.machineID);
			for (BlobBufferStatus bbs : bufferStatusData.blobsBufferStatusSet) {
				System.out.println("Blob - " + bbs.blobID);
				System.out.println("Input buffers...");
				for (BufferStatus bs : bbs.inputSet)
					System.out.println(bs);
				for (BufferStatus bs : bbs.outputSet)
					System.out.println(bs);
			}
		}
	}
}
