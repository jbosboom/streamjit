package edu.mit.streamjit.impl.distributed.profiler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.mit.streamjit.impl.distributed.profiler.ProfileElementLoggers.FileProfileElementLogger;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNBufferStatusData;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNProfileElementProcessor;

/**
 * Profiling data from all StreamNodes come to this central point.
 * 
 * @author sumanan
 * @since 27 Jan, 2015
 */
public class MasterProfiler implements SNProfileElementProcessor {

	// private final Map<Integer, SNBufferStatusData> BufferStatusDataMap;

	private final ProfileElementLogger logger;

	public ProfileElementLogger logger() {
		return logger;
	}

	public MasterProfiler(String appName) {
		// BufferStatusDataMap = new ConcurrentHashMap<>();
		logger = new FileProfileElementLogger(appName);
	}

	@Override
	public void process(SNBufferStatusData bufferStatusData) {
		// BufferStatusDataMap.put(bufferStatusData.machineID,
		// bufferStatusData);
		logger.process(bufferStatusData);
	}
}
