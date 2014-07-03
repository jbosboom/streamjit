package edu.mit.streamjit.util.bytecode;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.FluentIterable;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import com.google.common.reflect.Invokable;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.VoidType;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
	/**
	 * Created only for Methods that don't mirror methods of live Class objects.
	 */
	private final ParentedList<Method, LocalVariable> localVariables;
	public Method(java.lang.reflect.Method method, Klass parent) {
		super(parent.getParent().types().getMethodType(method), method.getName());
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.modifiers = Sets.immutableEnumSet(Modifier.fromMethodBits(Shorts.checkedCast(method.getModifiers())));
		//We're unresolved, so we don't have arguments or basic blocks.
		this.localVariables = null;
	}
	public Method(java.lang.reflect.Constructor<?> ctor, Klass parent) {
		super(parent.getParent().types().getMethodType(ctor), "<init>");
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.modifiers = Sets.immutableEnumSet(Modifier.fromMethodBits(Shorts.checkedCast(ctor.getModifiers())));
		//We're unresolved, so we don't have arguments or basic blocks.
		this.localVariables = null;
	}
	public Method(String name, MethodType type, Set<Modifier> modifiers, Klass parent) {
		super(type, name);
		if (name.equals("<init>"))
			checkArgument(type.getReturnType().equals(type.getTypeFactory().getType(parent)));
		if (name.equals("<clinit>")) {
			checkArgument(type.getReturnType() instanceof VoidType);
			checkArgument(type.getParameterTypes().size() == 0);
			checkArgument(modifiers.contains(Modifier.STATIC));
		}
		parent.methods().add(this);
		this.modifiers = modifiers;
		this.arguments = buildArguments();
		this.basicBlocks = new ParentedList<>(this, BasicBlock.class);
		this.localVariables = new ParentedList<>(this, LocalVariable.class);
	}

	public boolean isMutable() {
		return getParent().isMutable();
	}

	public Invokable<?, ?> getBackingInvokable() {
		//We don't call this very often (if at all), so look it up every time
		//rather than burn a field on all Methods.
		Class<?> klass = getParent().getBackingClass();
		if (klass == null) return null;
		MethodType type = getType();
		if (hasReceiver())
			type = type.dropFirstArgument();
		List<Class<?>> paramTypes = new ArrayList<>();
		for (RegularType t : type.getParameterTypes()) {
			Class<?> backingParamClass = t.getKlass().getBackingClass();
			//Live Methods can only have live Classes as parameter types.
			if (backingParamClass == null) return null;
			paramTypes.add(backingParamClass);
		}
		try {
			Class<?>[] array = paramTypes.toArray(new Class<?>[paramTypes.size()]);
			if (getName().equals("<init>"))
				return Invokable.from(klass.getDeclaredConstructor(array));
			else
				return Invokable.from(klass.getDeclaredMethod(getName(), array));
		} catch (NoSuchMethodException ex) {
			throw new AssertionError(String.format("Can't happen! Class %s doesn't have a %s(%s) method?", klass, getName(), paramTypes), ex);
		}
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

		this.arguments = buildArguments();
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

	public Argument getArgument(String name) {
		for (Argument a : arguments())
			if (a.getName().equals(name))
				return a;
		return null;
	}

	public List<BasicBlock> basicBlocks() {
		checkState(isResolved(), "not resolved: %s", this);
		return basicBlocks;
	}

	public List<LocalVariable> localVariables() {
		checkState(localVariables != null, "mirrors live Class object: %s", this);
		return localVariables;
	}

	public LocalVariable getLocalVariable(String name) {
		for (LocalVariable v : localVariables())
			if (v.getName().equals(name))
				return v;
		return null;
	}

	public boolean isConstructor() {
		return getName().equals("<init>");
	}

	public boolean hasReceiver() {
		return !(modifiers().contains(Modifier.STATIC) || isConstructor());
	}

	public boolean isSignaturePolymorphic() {
		return !getParent().isMutable()
				&& getParent().getName().equals("java.lang.invoke.MethodHandle")
				&& (getName().equals("invoke") || getName().equals("invokeExact"));
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

	public Method removeFromParent() {
		checkState(getParent() != null);
		getParent().methods().remove(this);
		return this;
	}

	public void eraseFromParent() {
		removeFromParent();
		for (BasicBlock b : ImmutableList.copyOf(basicBlocks))
			b.eraseFromParent();
	}

	@Override
	public String toString() {
		return modifiers.toString() + " " + getName() + " " +getType();
	}

	public void dump(OutputStream stream) {
		dump(new PrintWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8)));
	}

	public void dump(Writer writer) {
		dump(new PrintWriter(writer));
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
			writer.flush();
			return;
		}

		writer.write(" {");
		writer.println();
		if (isMutable()) {
			for (LocalVariable v : localVariables()) {
				writer.write("\t");
				writer.write(v.toString());
				writer.write(";");
				writer.println();
			}
			if (!localVariables.isEmpty())
				writer.println();
		}
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
		writer.write("}");
		writer.println();
		writer.flush();
	}

	private ImmutableList<Argument> buildArguments() {
		ImmutableList<RegularType> paramTypes = getType().getParameterTypes();
		ImmutableList.Builder<Argument> builder = ImmutableList.builder();
		if (isConstructor())
			builder.add(new Argument(this, getParent().getParent().types().getRegularType(getParent()), "this"));
		for (int i = 0; i < paramTypes.size(); ++i) {
			String name = (i == 0 && hasReceiver()) ? "this" : "arg"+i;
			builder.add(new Argument(this, paramTypes.get(i), name));
		}
		return builder.build();
	}
}
