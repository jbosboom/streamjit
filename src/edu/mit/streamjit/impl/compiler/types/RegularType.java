package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * A RegularType is a primitive or reference type (i.e., any non-void return
 * type).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class RegularType extends ReturnType {
	RegularType(Klass klass) {
		super(klass);
		checkArgument(!"void".equals(klass.getName()), "not a RegularType: %s", klass);
	}
}
