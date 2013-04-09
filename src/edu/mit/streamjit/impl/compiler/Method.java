package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.MethodType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.List;
import java.util.Set;

/**
 * Method represents an executable element of a class file (instance method,
 * class (static) method, instance initializer (constructor), or class (static)
 * initializer).
 *
 * Methods may be resolved or unresolved.  Resolved methods have basic blocks,
 * while unresolved methods are just declarations for generating call
 * instructions.  Methods mirroring actual methods of live Class objects are
 * created unresolved, while mutable Methods are created resolved but with an
 * empty list of basic blocks.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Method extends Value implements ParentedList.Parented<Klass> {
	@IntrusiveList.Next
	private Method next;
	@IntrusiveList.Previous
	private Method previous;
	@ParentedList.Parent
	private Klass parent;
	private final Set<Modifier> modifiers;
	/**
	 * Lazily initialized during resolution.
	 */
	private ImmutableList<Argument> arguments;
	/**
	 * Lazily initialized during resolution.
	 */
	private ParentedList<Method, BasicBlock> basicBlocks;
	public Method(java.lang.reflect.Method method, Klass parent) {
		super(parent.getParent().types().getMethodType(method), method.getName());
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.modifiers = Modifier.fromMethodBits(Shorts.checkedCast(method.getModifiers()));
		//We're unresolved, so we don't have arguments or basic blocks.
	}

	@Override
	public MethodType getType() {
		return (MethodType)super.getType();
	}

	@Override
	public Klass getParent() {
		return parent;
	}

	public ImmutableList<Argument> arguments() {
		return arguments;
	}

	public void add(BasicBlock block) {
		basicBlocks.add(block);
		block.setParent(this);
	}

	public void remove(BasicBlock block) {
		basicBlocks.remove(block);
		block.setParent(null);
	}

	public List<BasicBlock> basicBlocks() {
		return basicBlocks;
	}
}
