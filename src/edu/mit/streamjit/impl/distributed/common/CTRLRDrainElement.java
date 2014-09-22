package edu.mit.streamjit.impl.distributed.common;

import java.util.Set;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link Controller} and {@link StreamNode} shall communicate all kind of
 * draining information by exchanging DrainElement. </p> All fields of
 * DrainElement are public and final because the purpose of this class is
 * nothing but exchange data between controller and stream node..
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 29, 2013
 */
public abstract class CTRLRDrainElement implements CTRLRMessageElement {
	private static final long serialVersionUID = 1L;

	public abstract void process(CTRLRDrainProcessor dp);

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * {@link Controller} can send this to {@link StreamNode}s to get the
	 * drained data of the blobs. stream nodes which receive this object should
	 * reply with their drained data.
	 */
	public static final class DrainDataRequest extends CTRLRDrainElement {
		private static final long serialVersionUID = 1L;

		/**
		 * To avoid unnecessary communication overhead, {@link Controller} can
		 * send list of {@link Blob}s it needs drained data
		 */
		public final Set<Token> blobsSet;

		public DrainDataRequest(Set<Token> blobsSet) {
			this.blobsSet = blobsSet;
		}

		@Override
		public void process(CTRLRDrainProcessor dp) {
			dp.process(this);
		}
	}

	/**
	 * {@link Controller} shall send this object to command the
	 * {@link StreamNode}s to drian a particular {@link Blob}. </p>
	 * Unfortunately the name of this class became a verb. Anyway the purpose of
	 * sending an object of this class is to initiate a draining action.
	 */
	public static final class DoDrain extends CTRLRDrainElement {
		private static final long serialVersionUID = 1L;

		public final DrainType drainType;

		/**
		 * Identifies the blob. Since {@link Blob}s do not have an unique
		 * identifier them self, the minimum input token of that blob is used as
		 * identifier.
		 */
		public final Token blobID;

		public DoDrain(Token blobID, DrainType drainType) {
			this.blobID = blobID;
			this.drainType = drainType;
		}

		@Override
		public void process(CTRLRDrainProcessor dp) {
			dp.process(this);
		}
	}

	/**
	 * </p> As sub types of the {@link DrainElement} classes, not enums,
	 * overloaded methods in DrainProcessor is enough. Jvm will automatically
	 * dispatch the sub type of DrainElement to correct matching process() here.
	 * We do not need explicit processXXX() functions as it is done for all
	 * enums such as {@link Error}, {@link AppStatus} and {@link Request}.
	 */
	public interface CTRLRDrainProcessor {

		public void process(DrainDataRequest drnDataReq);

		public void process(DoDrain drain);
	}

	/**
	 * Three types of draining are possible.
	 * <ol>
	 * <li>drainType = 1 : Final draining. No drain data. All {@link Blob}s are
	 * expected to run and finish all data.
	 * <li>drainType = 2 : Intermediate draining. Drain data is required in this
	 * mode. {@link BoundaryInputChannel}s may create extra buffer and put all
	 * unconsumed data, and finally send this drain data to the
	 * {@link Controller} for reconfiguration.
	 * <li>drainType = 3 : Discard all unconsumed data. This is useful, if we
	 * don't care about the data while tuning for performance.
	 * </ol>
	 * 
	 * See the same description at {@link BoundaryInputChannel#stop(int)}.
	 * 
	 */
	public enum DrainType {
		FINAL(1), INTERMEDIATE(2), DISCARD(3);
		private final int code;

		DrainType(int code) {
			this.code = code;
		}

		public int toint() {
			return code;
		}
	}
}
