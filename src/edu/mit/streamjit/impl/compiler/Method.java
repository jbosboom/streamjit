package edu.mit.streamjit.impl.compiler;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.FluentIterable;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.impl.compiler.insts.Instruction;
import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.util.IntrusiveList;
import java.io.PrintWriter;
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
public class Method extends Value implements Accessible, Parented<Klass> {
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
			String name = hasReceiver() ? "this" : "arg"+i;
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

	public boolean isConstructor() {
		return getName().equals("<init>");
	}

	public boolean hasReceiver() {
		return !(modifiers().contains(Modifier.STATIC) || isConstructor());
	}

	@Override
	public Access getAccess() {
		return Access.fromModifiers(modifiers());
	}

	@Override
	public void setAccess(Access access) {
		modifiers().removeAll(Access.allAccessModifiers());
		modifiers().addAll(access.modifiers());
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

	public void dump(PrintWriter writer) {
		writer.write(Joiner.on(' ').join(modifiers()));
		writer.write(" ");
		writer.write(getType().getReturnType().toString());
		writer.write(" ");
		writer.write(getName());

		writer.write("(");
		String argString;
		if (isResolved())
			argString = Joiner.on(", ").join(FluentIterable.from(arguments()).transform(new Function<Argument, String>() {
				@Override
				public String apply(Argument input) {
					return input.getType()+" "+input.getName();
				}
			}));
		else
			argString = Joiner.on(", ").join(FluentIterable.from(getType().getParameterTypes()).transform(Functions.toStringFunction()));
		writer.write(argString);
		writer.write(")");

		if (!isResolved()) {
			writer.write(";");
			writer.println();
			return;
		}
		writer.write(" {");
		writer.println();

		for (BasicBlock b : basicBlocks()) {
			writer.write(b.getName());
			writer.write(": ");
			writer.println();

			for (Instruction i : b.instructions()) {
				writer.write("\t");
				writer.write(i.toString());
				writer.println();
			}
		}
	}
}
