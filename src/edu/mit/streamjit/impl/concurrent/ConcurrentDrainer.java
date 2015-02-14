package edu.mit.streamjit.impl.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Drainer for {@link ConcurrentStreamCompiler}. Traverse through
 * {@link BlobNode}s of the {@link BlobGraph} and performs the draining.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public final class ConcurrentDrainer extends AbstractDrainer {

	/**
	 * Each blob in the stream graph and it's blobID.
	 */
	ImmutableMap<Token, Blob> blobMap;

	/**
	 * Set of threads that each blob is having.
	 */
	ImmutableMap<Blob, Set<BlobThread>> threadMap;

	public ConcurrentDrainer(BlobGraph blobGraph,
			Map<Blob, Set<BlobThread>> threadMap) {
		setBlobGraph(blobGraph);
		blobMap = buildBlobMap(threadMap.keySet());
		this.threadMap = ImmutableMap.copyOf(threadMap);
	}

	@Override
	protected void drainingDone(boolean isFinal) {
		System.out.println("Draining Finished");
	}

	@Override
	protected void drain(Token blobID, DrainType drainType) {
		Blob blob = blobMap.get(blobID);
		checkNotNull(blob);

		Runnable callback = new DrainerCallBack(this, blobID);
		blob.drain(callback);
	}

	@Override
	protected void drainingDone(Token blobID, boolean isFinal) {
		Blob blob = blobMap.get(blobID);
		Set<BlobThread> blobThreads = threadMap.get(blob);
		checkNotNull(blobThreads);
		for (BlobThread thread : blobThreads)
			thread.requestStop();
	}

	private ImmutableMap<Token, Blob> buildBlobMap(Set<Blob> blobSet) {
		ImmutableMap.Builder<Token, Blob> builder = new ImmutableMap.Builder<>();
		for (Blob b : blobSet) {
			Token t = Utils.getBlobID(b);
			if (t == null)
				throw new AssertionError("Blob with no identifier");

			builder.put(t, b);
		}
		ImmutableMap<Token, Blob> blobMap = builder.build();

		if (!blobMap.keySet().equals(blobGraph.getBlobIds()))
			throw new AssertionError("Not all blob nodes have matching blobs");

		return blobMap;
	}

	private class DrainerCallBack implements Runnable {
		AbstractDrainer drainer;
		Token blobID;

		private DrainerCallBack(AbstractDrainer drainer, Token blobID) {
			this.drainer = drainer;
			this.blobID = blobID;
		}

		@Override
		public void run() {
			drainer.drained(blobID);
		}
	}

	@Override
	protected void prepareDraining(boolean isFinal) {
		// TODO Auto-generated method stub
	}
}
