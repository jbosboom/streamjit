package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.impl.common.MethodNodeBuilder;
import edu.mit.streamjit.impl.compiler.types.ReferenceType;
import edu.mit.streamjit.impl.compiler.types.Type;
import edu.mit.streamjit.impl.compiler.types.TypeFactory;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.IincInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.MultiANewArrayInsnNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 * Resolves methods.
 *
 * This class assumes it's parsing valid bytecode, so it asserts rather than
 * throws on simple checks like "aload_0 is loading a reference type".
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/12/2013
 */
public final class MethodResolver {
	public static void resolve(Method m) {
		new MethodResolver(m).resolve();
	}

	private final Method method;
	private final MethodNode methodNode;
	private final List<BBInfo> blocks = new ArrayList<>();
	private final TypeFactory typeFactory;
	private MethodResolver(Method m) {
		this.method = m;
		this.typeFactory = method.getType().getTypeFactory();
		try {
			this.methodNode = MethodNodeBuilder.buildMethodNode(method);
		} catch (IOException | NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void resolve() {
		findBlockBoundaries();
		for (BBInfo block : blocks)
			buildInstructions(block);
	}

	private void findBlockBoundaries() {
		InsnList insns = methodNode.instructions;
		int lastEnd = 0;
		for (int i = 0; i < insns.size(); ++i) {
			AbstractInsnNode insn = insns.get(i);
			int opcode = insn.getOpcode();
			if (insn instanceof JumpInsnNode || insn instanceof LookupSwitchInsnNode ||
					insn instanceof TableSwitchInsnNode || opcode == Opcodes.ATHROW ||
					opcode == Opcodes.IRETURN || opcode == Opcodes.LRETURN ||
					opcode == Opcodes.FRETURN || opcode == Opcodes.DRETURN ||
					opcode == Opcodes.ARETURN || opcode == Opcodes.RETURN) {
				int end = i+1;
				blocks.add(new BBInfo(lastEnd, end));
				lastEnd = end;
			}
		}
	}

	private void buildInstructions(BBInfo block) {
		FrameState frame = block.entryState.copy();
		for (int i = block.start; i < block.end; ++i) {
			AbstractInsnNode insn = methodNode.instructions.get(i);
			if (insn.getOpcode() == -1) continue;//pseudo-instruction node
			if (insn instanceof FieldInsnNode)
				interpret((FieldInsnNode)insn, frame, block);
			else if (insn instanceof IincInsnNode)
				interpret((IincInsnNode)insn, frame, block);
			else if (insn instanceof InsnNode)
				interpret((InsnNode)insn, frame, block);
			else if (insn instanceof IntInsnNode)
				interpret((IntInsnNode)insn, frame, block);
			else if (insn instanceof InvokeDynamicInsnNode)
				interpret((InvokeDynamicInsnNode)insn, frame, block);
			else if (insn instanceof JumpInsnNode)
				interpret((JumpInsnNode)insn, frame, block);
			else if (insn instanceof LdcInsnNode)
				interpret((LdcInsnNode)insn, frame, block);
			else if (insn instanceof LookupSwitchInsnNode)
				interpret((LookupSwitchInsnNode)insn, frame, block);
			else if (insn instanceof MethodInsnNode)
				interpret((MethodInsnNode)insn, frame, block);
			else if (insn instanceof MultiANewArrayInsnNode)
				interpret((MultiANewArrayInsnNode)insn, frame, block);
			else if (insn instanceof TableSwitchInsnNode)
				interpret((TableSwitchInsnNode)insn, frame, block);
			else if (insn instanceof TypeInsnNode)
				interpret((TypeInsnNode)insn, frame, block);
			else if (insn instanceof VarInsnNode)
				interpret((VarInsnNode)insn, frame, block);
		}
		//TODO: merge state with successors
	}

	private void interpret(FieldInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(IincInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(InsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(IntInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(InvokeDynamicInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(JumpInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(LdcInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(LookupSwitchInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(MethodInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(MultiANewArrayInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(TableSwitchInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(TypeInsnNode insn, FrameState frame, BBInfo block) {
		switch (insn.getOpcode()) {
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}
	private void interpret(VarInsnNode insn, FrameState frame, BBInfo block) {
		int var = insn.var;
		switch (insn.getOpcode()) {
			case Opcodes.ILOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(int.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.LLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(long.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.FLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(float.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.DLOAD:
				assert frame.locals[var].getType().isSubtypeOf(typeFactory.getType(double.class));
				frame.stack.push(frame.locals[var]);
				break;
			case Opcodes.ALOAD:
				assert frame.locals[var].getType() instanceof ReferenceType;
				frame.stack.push(frame.locals[var]);
				break;
			default:
				throw new UnsupportedOperationException(""+insn.getOpcode());
		}
	}

	private final class BBInfo {
		private final BasicBlock block;
		//The index of the first and one-past-the-last instructions.
		private final int start, end;
		private FrameState entryState;
		private final FrameNode frame;
		private BBInfo(int start, int end) {
			this.block = new BasicBlock(method.getParent().getParent());
			method.basicBlocks().add(this.block);
			this.start = start;
			this.end = end;
			if (start == 0) { //first block starts with args and empty stack
				this.entryState = new FrameState(methodNode.maxLocals);
				Value[] entryLocals = entryState.locals;
				int i = 0;
				//If the method is a constructor, it begins with an
				//UninitializedThis object in local variable 0.
				if (method.getName().equals("<init>"))
					entryLocals[i++] = new UninitializedValue(typeFactory.getType(method.getParent()), "uninitializedThis");
				for (Argument a : method.arguments()) {
					entryLocals[i] = a;
					Type argType = a.getType();
					if (argType.equals(typeFactory.getType(long.class)) ||
							argType.equals(typeFactory.getType(double.class)))
						i += 2;
					else
						++i;
				}
			}
			this.frame = findOnlyFrameNode();
		}

		private FrameNode findOnlyFrameNode() {
			FrameNode f = null;
			for (int i = start; i != end; ++i) {
				AbstractInsnNode insn = methodNode.instructions.get(i);
				if (insn instanceof FrameNode) {
					assert f == null : f + " " +insn;
					f = (FrameNode)insn;
				}
			}
			return f;
		}
	}

	private final class FrameState {
		private final Value[] locals;
		private final Deque<Value> stack;
		private FrameState(int localSize) {
			this.locals = new Value[localSize];
			this.stack = new ArrayDeque<>();
		}
		private FrameState copy() {
			FrameState s = new FrameState(locals.length);
			System.arraycopy(locals, 0, s.locals, 0, locals.length);
			return s;
		}
	}

	/**
	 * A dummy value used when building SSA form.  Exists only to get RAUW'd to
	 * the result of the constructor call.
	 *
	 * Has the type of the object under construction.
	 */
	private static class UninitializedValue extends Value {
		private UninitializedValue(Type type, String name) {
			super(type, name);
		}
	}

	public static void main(String[] args) {
		Module m = new Module();
		Klass k = m.getKlass(Identity.class);
		k.getMethods("work").iterator().next().resolve();
	}
}
