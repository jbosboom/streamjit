/**
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
/**
 * 
 */
package edu.mit.streamjit.partitioner;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.interp.Interpreter;

public class OpentunerMeasurement {

	/**
	 * @param args
	 */
	public static void main(String[] args) {	
		
		
	}
	
	/**
	 * Executes all blobs on same machine.
	 * TODO: Need to implement distributed execution.
	 */
	private void executeBlobs(BlobFactory blobFactory)
	{
		/*List<Blob> blobList = blobFactory.getBlobList();
		ExecutorService threadPool = Executors.newFixedThreadPool(blobList.size());
		for (Blob b : blobList)
		{
			threadPool.submit(b.getCoreCode(0));			
		}*/
				
	}	
}
