/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.Worker;
import java.util.List;

/**
 * See Workers for details.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/5/2013
 */
public abstract class Portals {
	public static List<Worker<?, ?>> getRecipients(Portal<?> portal) {
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
	protected abstract List<Worker<?, ?>> getRecipients_impl(Portal<?> portal);
	protected abstract void setConstraints_impl(Portal<?> portal, List<MessageConstraint> constraints);
	//</editor-fold>
}
