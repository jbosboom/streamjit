package edu.mit.streamjit.impl.concurrent;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.BlobGraph.AbstractDrainer;
import edu.mit.streamjit.impl.common.BlobGraph;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.common.BlobGraph.BlobNode;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Drainer for {@link ConcurrentStreamCompiler}. traverse through
 * {@link BlobNode}s of the {@link BlobGraph} and performs the draining.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public final class ConcurrentDrainer extends AbstractDrainer {

	/**
	 * Each blob is mapped to a blobnode in the blob graph.
	 */
	ImmutableMap<BlobNode, Blob> blobMap;
	/**
	 * Set of threads that each blob is having.
	 */
	ImmutableMap<Blob, Set<BlobThread>> threadMap;

	public ConcurrentDrainer(BlobGraph blobGraph, boolean needDrainData,
			Map<Blob, Set<BlobThread>> threadMap) {
		super(blobGraph, needDrainData);
		Set<Blob> blobSet = threadMap.keySet();
		blobMap = buildBlobMap(blobGraph.getBlobNodes(), blobSet);
		this.threadMap = ImmutableMap.copyOf(threadMap);
	}

	private ImmutableMap<BlobNode, Blob> buildBlobMap(
			Set<BlobNode> blobNodeSet, Set<Blob> blobSet) {
		ImmutableMap.Builder<BlobNode, Blob> builder = new ImmutableMap.Builder<>();
		for (Blob b : blobSet) {
			Token t = Utils.getBlobID(b);
			if (t == null)
				throw new AssertionError("Blob with no identifier");

			BlobNode node = getBlobNode(blobNodeSet, t);
			checkNotNull(node);

			builder.put(node, b);
		}
		ImmutableMap<BlobNode, Blob> blobMap = builder.build();

		if (!blobMap.keySet().equals(blobNodeSet))
			throw new AssertionError("Not all blob nodes have matching blobs");

		return blobMap;
	}

	private BlobNode getBlobNode(Set<BlobNode> blobNodeSet, Token t) {
		for (BlobNode node : blobNodeSet) {
			if (node.getBlobID().equals(t))
				return node;
		}
		return null;
	}

	@Override
	protected void drain(BlobNode node) {
		checkNotNull(node);
		Blob blob = blobMap.get(node);
		checkNotNull(blob);

		Runnable callback = new DrainerCallBack(node);
		blob.drain(callback);
	}

	@Override
	protected void drained(BlobNode node) {
		Blob blob = blobMap.get(node);
		Set<BlobThread> blobThreads = threadMap.get(blob);
		checkNotNull(blobThreads);
		for (BlobThread thread : blobThreads)
			thread.requestStop();
	}

	private class DrainerCallBack implements Runnable {
		BlobNode node;

		private DrainerCallBack(BlobNode node) {
			this.node = node;
		}

		@Override
		public void run() {
			node.drained();
		}
	}

	@Override
	protected void drainingFinished() {
		System.out.println("Draining Finished");
	}
}
