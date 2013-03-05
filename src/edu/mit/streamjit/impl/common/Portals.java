package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.MessageConstraint;
import edu.mit.streamjit.Portal;
import edu.mit.streamjit.PrimitiveWorker;
import java.util.List;

/**
 * See Workers for details.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/5/2013
 */
public abstract class Portals {
	public static List<PrimitiveWorker<?, ?>> getRecipients(Portal<?> portal) {
		return FRIEND.getRecipients_impl(portal);
	}
	public static void setConstraints(Portal<?> portal, List<MessageConstraint> constraints) {
		FRIEND.setConstraints_impl(portal, constraints);
	}

	//<editor-fold defaultstate="collapsed" desc="Friend pattern support">
	protected Portals() {}
	private static Portals FRIEND;
	protected static void setFriend(Portals portals) {
		if (FRIEND != null)
			throw new AssertionError("Can't happen: two friends?");
		FRIEND = portals;
	}
	static {
		try {
			//Ensure Portal is initialized.
			Class.forName(Portal.class.getName(), true, Portal.class.getClassLoader());
		} catch (ClassNotFoundException ex) {
			throw new AssertionError(ex);
		}
	}
	protected abstract List<PrimitiveWorker<?, ?>> getRecipients_impl(Portal<?> portal);
	protected abstract void setConstraints_impl(Portal<?> portal, List<MessageConstraint> constraints);
	//</editor-fold>
}
