package edu.mit.streamjit.impl.distributed.profiler;

import java.util.HashMap;
import java.util.Map;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.BufferStatusData;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNProfileElementProcessor;

/**
 * Profiling data from all StreamNodes come to this central point.
 * 
 * @author sumanan
 * @since 27 Jan, 2015
 */
public class MasterProfiler implements SNProfileElementProcessor {

	private final Map<Token, BufferStatusData> BufferStatusDataMap;

	public MasterProfiler() {
		BufferStatusDataMap = new HashMap<>();
	}

	@Override
	public void process(BufferStatusData bufferStatusData) {
		BufferStatusDataMap.put(bufferStatusData.blobID, bufferStatusData);
	}
}
