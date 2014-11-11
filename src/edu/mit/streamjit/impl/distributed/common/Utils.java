package edu.mit.streamjit.impl.distributed.common;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.IOInfo;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class Utils {

	public static Token getBlobID(Blob b) {
		return Collections.min(b.getInputs());
	}

	public static Token getblobID(Set<Worker<?, ?>> workers) {
		ImmutableSet.Builder<Token> inputBuilder = new ImmutableSet.Builder<>();
		for (IOInfo info : IOInfo.externalEdges(workers)) {
			if (info.isInput())
				inputBuilder.add(info.token());
		}

		return Collections.min(inputBuilder.build());
	}

	/**
	 * Prints heapMaxSize, current heapSize and heapFreeSize.
	 */
	public static void printMemoryStatus() {
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		long heapSize = Runtime.getRuntime().totalMemory();
		long heapFreeSize = Runtime.getRuntime().freeMemory();
		System.out.println("##############################################");
		System.out.println("heapMaxSize = " + heapMaxSize / 1e6);
		System.out.println("heapSize = " + heapSize / 1e6);
		System.out.println("heapFreeSize = " + heapFreeSize / 1e6);
		System.out.println("##############################################");
	}
}
