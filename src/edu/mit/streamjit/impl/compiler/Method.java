package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.impl.compiler.types.RegularType;
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
public class Method extends Value implements Parented<Klass> {
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
		this.modifiers = Sets.immutableEnumSet(Modifier.fromMethodBits(Shorts.checkedCast(method.getModifiers())));
		//We're unresolved, so we don't have arguments or basic blocks.
	}
	public Method(java.lang.reflect.Constructor<?> ctor, Klass parent) {
		super(parent.getParent().types().getMethodType(ctor), "<init>");
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.modifiers = Sets.immutableEnumSet(Modifier.fromMethodBits(Shorts.checkedCast(ctor.getModifiers())));
		//We're unresolved, so we don't have arguments or basic blocks.
	}

	public boolean isMutable() {
		return getParent().isMutable();
	}

	public boolean isResolved() {
		return basicBlocks != null;
	}

	/**
	 * Returns true iff this method can be resolved.  This method is safe to
	 * call at all times after this Method's construction is complete, even if
	 * this method has already been resolved.
	 * @return true iff this method can be resolved
	 */
	public boolean isResolvable() {
		//Abstract methods don't have code; native methods have code, but not of
		//a form we can parse.
		return !modifiers().contains(Modifier.ABSTRACT) && !modifiers.contains(Modifier.NATIVE);
	}

	public void resolve() {
		checkState(isResolvable(), "cannot resolve %s", this);
		if (isResolved())
			return;

		ImmutableList<RegularType> paramTypes = getType().getParameterTypes();
		ImmutableList.Builder<Argument> builder = ImmutableList.builder();
		for (int i = 0; i < paramTypes.size(); ++i) {
			//TODO: I'm not sure if ctors are static, but they don't have a this
			//parameter from our perspective.
			String name = (i == 0 && !modifiers().contains(Modifier.STATIC)) ? "this" : "arg"+i;
			builder.add(new Argument(this, paramTypes.get(i), name));
		}
		this.arguments = builder.build();
		this.basicBlocks = new ParentedList<>(this, BasicBlock.class);
		MethodResolver.resolve(this);
	}

	@Override
	public MethodType getType() {
		return (MethodType)super.getType();
	}

	public Set<Modifier> modifiers() {
		return modifiers;
	}

	public ImmutableList<Argument> arguments() {
		checkState(isResolved(), "not resolved: %s", this);
		return arguments;
	}

	public List<BasicBlock> basicBlocks() {
		checkState(isResolved(), "not resolved: %s", this);
		return basicBlocks;
	}

	@Override
	public void setName(String name) {
		checkState(isMutable(), "can't change name of method on immutable class %s", getParent());
		super.setName(name);
	}

	@Override
	public Klass getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return modifiers.toString() + " " + getName() + " " +getType();
	}
}
