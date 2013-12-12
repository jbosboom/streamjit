package edu.mit.streamjit.util.bytecode;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.util.bytecode.insts.ArrayLengthInst;
import edu.mit.streamjit.util.bytecode.insts.ArrayLoadInst;
import edu.mit.streamjit.util.bytecode.insts.ArrayStoreInst;
import edu.mit.streamjit.util.bytecode.insts.BinaryInst;
import edu.mit.streamjit.util.bytecode.insts.BranchInst;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.InstanceofInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.JumpInst;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.NewArrayInst;
import edu.mit.streamjit.util.bytecode.insts.PhiInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import edu.mit.streamjit.util.bytecode.insts.SwitchInst;
import edu.mit.streamjit.util.bytecode.insts.TerminatorInst;
import edu.mit.streamjit.util.bytecode.insts.ThrowInst;
import edu.mit.streamjit.util.bytecode.types.ArrayType;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import edu.mit.streamjit.util.bytecode.types.NullType;
import edu.mit.streamjit.util.bytecode.types.PrimitiveType;
import edu.mit.streamjit.util.bytecode.types.ReferenceType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.ReturnType;
import edu.mit.streamjit.util.bytecode.types.Type;
import edu.mit.streamjit.util.bytecode.types.TypeFactory;
import edu.mit.streamjit.util.bytecode.types.VoidType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.MultiANewArrayInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 * Builds bytecode from methods.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/17/2013
 */
public final class MethodUnresolver {
	public static MethodNode unresolve(Method m) {
		checkNotNull(m);
		//Unresolving immutable methods (live Class methods) is only useful
		//during testing.
		//checkArgument(m.isMutable(), "unresolving immutable method %s", m);
		if (!m.modifiers().contains(Modifier.ABSTRACT))
			checkArgument(m.isResolved(), "unresolving unresolved method %s", m);
		return new MethodUnresolver(m).unresolve();
	}

	private final Method method;
	private final MethodNode methodNode;
	private final Map<Value, Integer> registers;
	private final Map<BasicBlock, LabelNode> labels;
	private final PrimitiveType booleanType, byteType, charType, shortType,
			intType, longType, floatType, doubleType;
	private MethodUnresolver(Method m) {
		this.method = m;
		this.methodNode = new MethodNode(Opcodes.ASM4);
		this.registers = new IdentityHashMap<>();
		this.labels = new IdentityHashMap<>();
		TypeFactory tf = m.getParent().getParent().types();
		this.booleanType = tf.getPrimitiveType(boolean.class);
		this.byteType = tf.getPrimitiveType(byte.class);
		this.charType = tf.getPrimitiveType(char.class);
		this.shortType = tf.getPrimitiveType(short.class);
		this.intType = tf.getPrimitiveType(int.class);
		this.longType = tf.getPrimitiveType(long.class);
		this.floatType = tf.getPrimitiveType(float.class);
		this.doubleType = tf.getPrimitiveType(double.class);
	}

	public MethodNode unresolve() {
		this.methodNode.access = Modifier.toBits(method.modifiers());
		this.methodNode.name = method.getName();
		this.methodNode.desc = methodDescriptor(method);
		this.methodNode.exceptions = Collections.emptyList();

		if (!method.modifiers().contains(Modifier.ABSTRACT)) {
			allocateRegisters();
			createLabels();
			for (BasicBlock b : method.basicBlocks())
				methodNode.instructions.add(emit(b));
			peepholeOptimizations();
			int maxRegister = registers.values().isEmpty() ? 0 : Collections.max(registers.values());
			this.methodNode.maxLocals = maxRegister+2;
			//We'd like to use ClassWriter's COMPUTE_MAXS option to compute this
			//for us, but we also want to use CheckClassAdapter before
			//ClassWriter to get useful errors.  But CheckClassAdapter will
			//raise an error if we get this too low and run out of memory if we
			//just say Short.MAX_VALUE.  I think any bytecode can push at most
			//+2 net, so conservatively try 2*instructions.size().  Note that
			//this counts labels and other pseudo-instructions, as well as
			//instructions that have obviously no stack increase or a decrease.
			this.methodNode.maxStack = Math.min(2*methodNode.instructions.size(), Short.MAX_VALUE);
			buildLocalVariableTable();
		}

		return methodNode;
	}

	private void allocateRegisters() {
		//We allocate one or two registers (depending on type category) to each
		//instruction producing a non-void value, the method arguments, and the
		//local variables.
		int regNum = 0;
		for (Argument a : method.arguments()) {
			registers.put(a, regNum);
			regNum += a.getType().getCategory();
		}
		if (method.isMutable())
			for (LocalVariable v : method.localVariables()) {
				registers.put(v, regNum);
				regNum += v.getType().getFieldType().getCategory();
			}
		for (BasicBlock b : method.basicBlocks())
			for (Instruction i : b.instructions())
				if (!(i.getType() instanceof VoidType)) {
					registers.put(i, regNum);
					regNum += i.getType().getCategory();
				}
	}

	private void createLabels() {
		for (BasicBlock b : method.basicBlocks())
			labels.put(b, new LabelNode(new Label()));
	}

	private void buildLocalVariableTable() {
		LabelNode first = new LabelNode(), last = new LabelNode();
		methodNode.instructions.insert(first);
		methodNode.instructions.add(last);
		methodNode.localVariables = new ArrayList<>(registers.size());
		for (Map.Entry<Value, Integer> r : registers.entrySet()) {
			RegularType type = r.getKey() instanceof LocalVariable ?
					((LocalVariable)r.getKey()).getType().getFieldType() :
					(RegularType)r.getKey().getType();
			methodNode.localVariables.add(new LocalVariableNode(
					r.getKey().getName(),
					type.getDescriptor(),
					null,
					first,
					last,
					r.getValue()));
		}
	}

	private InsnList emit(BasicBlock block) {
		FluentIterable<TerminatorInst> terminators = FluentIterable.from(block.instructions()).filter(TerminatorInst.class);
		if (terminators.isEmpty())
			throw new IllegalArgumentException("block "+block.getName()+" in method "+block.getParent().getName()+" lacks a terminator");
		if (terminators.size() > 1)
			throw new IllegalArgumentException("block "+block.getName()+" in method "+block.getParent().getName()+" has multiple terminators: "+terminators);

		InsnList insns = new InsnList();
		insns.add(labels.get(block));
		for (Instruction i : block.instructions()) {
			if (i instanceof TerminatorInst)
				emitPhiMoves(block, insns);

			if (i instanceof ArrayLengthInst)
				emit((ArrayLengthInst)i, insns);
			else if (i instanceof ArrayLoadInst)
				emit((ArrayLoadInst)i, insns);
			else if (i instanceof ArrayStoreInst)
				emit((ArrayStoreInst)i, insns);
			else if (i instanceof BinaryInst)
				emit((BinaryInst)i, insns);
			else if (i instanceof BranchInst)
				emit((BranchInst)i, insns);
			else if (i instanceof CallInst)
				emit((CallInst)i, insns);
			else if (i instanceof CastInst)
				emit((CastInst)i, insns);
			else if (i instanceof InstanceofInst)
				emit((InstanceofInst)i, insns);
			else if (i instanceof JumpInst)
				emit((JumpInst)i, insns);
			else if (i instanceof LoadInst)
				emit((LoadInst)i, insns);
			else if (i instanceof NewArrayInst)
				emit((NewArrayInst)i, insns);
			else if (i instanceof PhiInst)
				//PhiInst deliberately omitted
				;
			else if (i instanceof ReturnInst)
				emit((ReturnInst)i, insns);
			else if (i instanceof StoreInst)
				emit((StoreInst)i, insns);
			else if (i instanceof SwitchInst)
				emit((SwitchInst)i, insns);
			else if (i instanceof ThrowInst)
				emit((ThrowInst)i, insns);
			else
				throw new AssertionError("can't emit "+i);
		}
		return insns;
	}

	private void emit(ArrayLengthInst i, InsnList insns) {
		load(i.getOperand(0), insns);
		insns.add(new InsnNode(Opcodes.ARRAYLENGTH));
		store(i, insns);
	}
	private void emit(ArrayLoadInst i, InsnList insns) {
		load(i.getArray(), insns);
		load(i.getIndex(), insns);
		if (i.getType() instanceof ReferenceType)
			insns.add(new InsnNode(Opcodes.AALOAD));
		else if (i.getType().equals(booleanType) || i.getType().equals(byteType))
			insns.add(new InsnNode(Opcodes.BALOAD));
		else if (i.getType().equals(charType))
			insns.add(new InsnNode(Opcodes.CALOAD));
		else if (i.getType().equals(shortType))
			insns.add(new InsnNode(Opcodes.SALOAD));
		else if (i.getType().equals(intType))
			insns.add(new InsnNode(Opcodes.IALOAD));
		else if (i.getType().equals(longType))
			insns.add(new InsnNode(Opcodes.LALOAD));
		else if (i.getType().equals(floatType))
			insns.add(new InsnNode(Opcodes.FALOAD));
		else if (i.getType().equals(doubleType))
			insns.add(new InsnNode(Opcodes.DALOAD));
		else
			throw new AssertionError(i);
		store(i, insns);
	}
	private void emit(ArrayStoreInst i, InsnList insns) {
		load(i.getArray(), insns);
		load(i.getIndex(), insns);
		load(i.getData(), insns);
		//TODO: what if the array is a null constant?
		RegularType componentType = ((ArrayType)i.getArray().getType()).getComponentType();
		if (componentType instanceof ReferenceType)
			insns.add(new InsnNode(Opcodes.AASTORE));
		else if (componentType.equals(booleanType) || componentType.equals(byteType))
			insns.add(new InsnNode(Opcodes.BASTORE));
		else if (componentType.equals(charType))
			insns.add(new InsnNode(Opcodes.CASTORE));
		else if (componentType.equals(shortType))
			insns.add(new InsnNode(Opcodes.SASTORE));
		else if (componentType.equals(intType))
			insns.add(new InsnNode(Opcodes.IASTORE));
		else if (componentType.equals(longType))
			insns.add(new InsnNode(Opcodes.LASTORE));
		else if (componentType.equals(floatType))
			insns.add(new InsnNode(Opcodes.FASTORE));
		else if (componentType.equals(doubleType))
			insns.add(new InsnNode(Opcodes.DASTORE));
		else
			throw new AssertionError(i);
	}
	private void emit(BinaryInst i, InsnList insns) {
		load(i.getOperand(0), insns);
		load(i.getOperand(1), insns);
		int opcode = 0;
		if (i.getOperand(0).getType().isSubtypeOf(intType)) {
			switch (i.getOperation()) {
				case ADD:
					opcode = Opcodes.IADD;
					break;
				case SUB:
					opcode = Opcodes.ISUB;
					break;
				case MUL:
					opcode = Opcodes.IMUL;
					break;
				case DIV:
					opcode = Opcodes.IDIV;
					break;
				case REM:
					opcode = Opcodes.IREM;
					break;
				case SHL:
					opcode = Opcodes.ISHL;
					break;
				case SHR:
					opcode = Opcodes.ISHR;
					break;
				case USHR:
					opcode = Opcodes.ISHR;
					break;
				case AND:
					opcode = Opcodes.IAND;
					break;
				case OR:
					opcode = Opcodes.IOR;
					break;
				case XOR:
					opcode = Opcodes.IXOR;
					break;
				default:
					throw new AssertionError(i);
			}
		} else if (i.getOperand(0).getType().equals(longType)) {
			switch (i.getOperation()) {
				case ADD:
					opcode = Opcodes.LADD;
					break;
				case SUB:
					opcode = Opcodes.LSUB;
					break;
				case MUL:
					opcode = Opcodes.LMUL;
					break;
				case DIV:
					opcode = Opcodes.LDIV;
					break;
				case REM:
					opcode = Opcodes.LREM;
					break;
				case SHL:
					opcode = Opcodes.LSHL;
					break;
				case SHR:
					opcode = Opcodes.LSHR;
					break;
				case USHR:
					opcode = Opcodes.LSHR;
					break;
				case AND:
					opcode = Opcodes.LAND;
					break;
				case OR:
					opcode = Opcodes.LOR;
					break;
				case XOR:
					opcode = Opcodes.LXOR;
					break;
				case CMP:
					opcode = Opcodes.LCMP;
					break;
				default:
					throw new AssertionError(i);
			}
		} else if (i.getOperand(0).getType().equals(floatType)) {
			switch (i.getOperation()) {
				case ADD:
					opcode = Opcodes.FADD;
					break;
				case SUB:
					opcode = Opcodes.FSUB;
					break;
				case MUL:
					opcode = Opcodes.FMUL;
					break;
				case DIV:
					opcode = Opcodes.FDIV;
					break;
				case REM:
					opcode = Opcodes.FREM;
					break;
				case CMP:
					opcode = Opcodes.FCMPL;
					break;
				case CMPG:
					opcode = Opcodes.FCMPG;
					break;
				default:
					throw new AssertionError(i);
			}
		} else if (i.getOperand(0).getType().equals(doubleType)) {
			switch (i.getOperation()) {
				case ADD:
					opcode = Opcodes.DADD;
					break;
				case SUB:
					opcode = Opcodes.DSUB;
					break;
				case MUL:
					opcode = Opcodes.DMUL;
					break;
				case DIV:
					opcode = Opcodes.DDIV;
					break;
				case REM:
					opcode = Opcodes.DREM;
					break;
				case CMP:
					opcode = Opcodes.DCMPL;
					break;
				case CMPG:
					opcode = Opcodes.DCMPG;
					break;
				default:
					throw new AssertionError(i);
			}
		} else
			throw new AssertionError(i);
		insns.add(new InsnNode(opcode));
		store(i, insns);
	}
	private void emit(BranchInst i, InsnList insns) {
		//TODO: accessor methods on BranchInst
		Value left = i.getOperand(0), right = i.getOperand(1);
		BasicBlock target = (BasicBlock)i.getOperand(2), fallthrough = (BasicBlock)i.getOperand(3);
		if (!method.basicBlocks().contains(target))
			throw new IllegalArgumentException("Branch targets block not in method: "+i);
		if (!method.basicBlocks().contains(fallthrough))
			throw new IllegalArgumentException("Branch falls through to block not in method: "+i);
		load(i.getOperand(0), insns);
		load(i.getOperand(1), insns);
		//TODO: long, float, doubles need to go through CMP inst first
		int opcode;
		if (left.getType() instanceof ReferenceType || left.getType() instanceof VoidType) {
			switch (i.getSense()) {
				case EQ:
					opcode = Opcodes.IF_ACMPEQ;
					break;
				case NE:
					opcode = Opcodes.IF_ACMPNE;
					break;
				default:
					throw new AssertionError(i);
			}
		} else if (left.getType().isSubtypeOf(intType)) {
			switch (i.getSense()) {
				case EQ:
					opcode = Opcodes.IF_ICMPEQ;
					break;
				case NE:
					opcode = Opcodes.IF_ICMPNE;
					break;
				case LT:
					opcode = Opcodes.IF_ICMPLT;
					break;
				case GT:
					opcode = Opcodes.IF_ICMPGT;
					break;
				case LE:
					opcode = Opcodes.IF_ICMPLE;
					break;
				case GE:
					opcode = Opcodes.IF_ICMPGE;
					break;
				default:
					throw new AssertionError(i);
			}
		} else
			throw new AssertionError(i);
		insns.add(new JumpInsnNode(opcode, labels.get(target)));
		insns.add(new JumpInsnNode(Opcodes.GOTO, labels.get(fallthrough)));
	}
	private void emit(CallInst i, InsnList insns) {
		Method m = i.getMethod();
		boolean callingSuperCtor = false;
		if (m.isConstructor()) {
			//If we're calling super(), load this.
			//TODO: this will get confused if we call a superclass constructor
			//for any reason other than our own initialization!
			if (method.isConstructor() && method.getParent().getSuperclass().equals(m.getParent())) {
				load(method.arguments().get(0), insns);
				callingSuperCtor = true;
			} else {
				insns.add(new TypeInsnNode(Opcodes.NEW, internalName(m.getType().getReturnType().getKlass())));
				insns.add(new InsnNode(Opcodes.DUP));
			}
		}
		int opcode;
		if (m.modifiers().contains(Modifier.STATIC))
			opcode = Opcodes.INVOKESTATIC;
		else if (m.isConstructor() ||
				m.getAccess().equals(Access.PRIVATE) ||
				//We're calling a superclass method we've overridden.
				(Iterables.contains(method.getParent().superclasses(), m.getParent())) &&
				method.getParent().getMethodByVirtual(m.getName(), m.getType()) != m)
			opcode = Opcodes.INVOKESPECIAL;
		else if (m.getParent().modifiers().contains(Modifier.INTERFACE))
			//TODO: may not be correct?
			opcode = Opcodes.INVOKEINTERFACE;
		else
			opcode = Opcodes.INVOKEVIRTUAL;

		String owner = internalName(m.getParent());
		//hack to make cloning arrays work
		if (opcode == Opcodes.INVOKESPECIAL && m.getName().equals("clone") && i.getArgument(0).getType() instanceof ArrayType) {
			opcode = Opcodes.INVOKEVIRTUAL;
			owner = internalName(((ArrayType)i.getArgument(0).getType()).getKlass());
		}

		for (Value v : i.arguments())
			load(v, insns);
		insns.add(new MethodInsnNode(opcode, owner, m.getName(), i.callDescriptor()));

		if (!(i.getType() instanceof VoidType) && !callingSuperCtor)
			store(i, insns);
	}
	private void emit(CastInst i, InsnList insns) {
		load(i.getOperand(0), insns);
		if (i.getType() instanceof ReferenceType) {
			insns.add(new TypeInsnNode(Opcodes.CHECKCAST, internalName(((ReferenceType)i.getType()).getKlass())));
		} else {
			PrimitiveType from = (PrimitiveType)i.getOperand(0).getType();
			PrimitiveType to = (PrimitiveType)i.getType();
			for (int op : from.getCastOpcode(to))
				insns.add(new InsnNode(op));
		}
		store(i, insns);
	}
	private void emit(InstanceofInst i, InsnList insns) {
		load(i.getOperand(0), insns);
		insns.add(new TypeInsnNode(Opcodes.INSTANCEOF, internalName(i.getTestType().getKlass())));
		store(i, insns);
	}
	private void emit(JumpInst i, InsnList insns) {
		BasicBlock target = (BasicBlock)i.getOperand(0);
		if (!method.basicBlocks().contains(target))
			throw new IllegalArgumentException("Jump to block not in method: "+i);
		insns.add(new JumpInsnNode(Opcodes.GOTO, labels.get(target)));
	}
	private void emit(LoadInst i, InsnList insns) {
		Value location = i.getLocation();
		if (location instanceof LocalVariable) {
			load(location, insns);
			store(i, insns);
		} else {
			Field f = (Field)location;
			if (!f.isStatic())
				load(i.getInstance(), insns);
			insns.add(new FieldInsnNode(
					f.isStatic() ? Opcodes.GETSTATIC : Opcodes.GETFIELD,
					internalName(f.getParent()),
					f.getName(),
					f.getType().getFieldType().getDescriptor()));
			store(i, insns);
		}
	}
	private void emit(NewArrayInst i, InsnList insns) {
		ArrayType t = i.getType();
		if (t.getDimensions() == 1) {
			load(i.getOperand(0), insns);
			RegularType ct = t.getComponentType();
			if (ct instanceof PrimitiveType) {
				if (ct.equals(booleanType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_BOOLEAN));
				else if (ct.equals(byteType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_BYTE));
				else if (ct.equals(charType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_CHAR));
				else if (ct.equals(shortType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_SHORT));
				else if (ct.equals(intType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_INT));
				else if (ct.equals(longType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_LONG));
				else if (ct.equals(floatType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_FLOAT));
				else if (ct.equals(doubleType))
					insns.add(new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_DOUBLE));
			} else {
				insns.add(new TypeInsnNode(Opcodes.ANEWARRAY, internalName(ct.getKlass())));
			}
		} else {
			for (Value v : i.operands())
				load(v, insns);
			insns.add(new MultiANewArrayInsnNode(t.getDescriptor(), i.getNumOperands()));
		}
		store(i, insns);
	}
	private void emit(ReturnInst i, InsnList insns) {
		ReturnType rt = i.getReturnType();
		if (rt instanceof VoidType)
			insns.add(new InsnNode(Opcodes.RETURN));
		else {
			load(i.getOperand(0), insns);
			if (rt instanceof ReferenceType)
				insns.add(new InsnNode(Opcodes.ARETURN));
			else if (rt.isSubtypeOf(intType))
				insns.add(new InsnNode(Opcodes.IRETURN));
			else if (rt.equals(longType))
				insns.add(new InsnNode(Opcodes.LRETURN));
			else if (rt.equals(floatType))
				insns.add(new InsnNode(Opcodes.FRETURN));
			else if (rt.equals(doubleType))
				insns.add(new InsnNode(Opcodes.DRETURN));
			else
				throw new AssertionError(i);
		}
	}
	private void emit(StoreInst i, InsnList insns) {
		Value location = i.getLocation();
		if (location instanceof LocalVariable) {
			load(i.getData(), insns);
			store(location, insns);
		} else {
			Field f = (Field)location;
			if (!f.isStatic())
				load(i.getInstance(), insns);
			load(i.getData(), insns);
			insns.add(new FieldInsnNode(
					f.isStatic() ? Opcodes.PUTSTATIC : Opcodes.PUTFIELD,
					internalName(f.getParent()),
					f.getName(),
					f.getType().getFieldType().getDescriptor()));
		}
	}
	private void emit(SwitchInst i, InsnList insns) {
		load(i.getValue(), insns);
		LookupSwitchInsnNode insn = new LookupSwitchInsnNode(null, null, null);
		insn.dflt = labels.get(i.getDefault());
		Iterator<Constant<Integer>> cases = i.cases().iterator();
		Iterator<BasicBlock> targets = i.successors().iterator();
		while (cases.hasNext()) {
			insn.keys.add(cases.next().getConstant());
			insn.labels.add(labels.get(targets.next()));
		}
		insns.add(insn);
	}
	private void emit(ThrowInst i, InsnList insns) {
		load(i.getOperand(0), insns);
		insns.add(new InsnNode(Opcodes.ATHROW));
	}

	private void emitPhiMoves(BasicBlock block, InsnList insns) {
		//In case phi instructions refer to one another, load all values onto
		//the operand stack, then store all at once.
		Deque<Value> pendingStores = new ArrayDeque<>();
		for (BasicBlock b : block.successors())
			for (Instruction i : b.instructions())
				if (i instanceof PhiInst) {
					PhiInst p = (PhiInst)i;
					Value ourDef = p.get(block);
					if (ourDef != null) {
						load(ourDef, insns);
						pendingStores.push(p);
					}
				}
		while (!pendingStores.isEmpty())
			store(pendingStores.pop(), insns);
	}

	private void load(Value v, InsnList insns) {
		if (v instanceof Constant) {
			Object c = ((Constant<?>)v).getConstant();
			if (c == null)
				insns.add(new InsnNode(Opcodes.ACONST_NULL));
			else if (c instanceof Class)
				insns.add(new LdcInsnNode(org.objectweb.asm.Type.getType((Class)c)));
			else if (c instanceof Boolean)
				insns.add(loadIntegerConstant(((Boolean)c) ? 1 : 0));
			else if (c instanceof Character)
				insns.add(loadIntegerConstant((int)((Character)c).charValue()));
			else if (c instanceof Byte || c instanceof Short || c instanceof Integer)
				insns.add(loadIntegerConstant(((Number)c).intValue()));
			else if (c instanceof Long)
				insns.add(loadLongConstant((Long)c));
			else if (c instanceof Float)
				insns.add(loadFloatConstant((Float)c));
			else if (c instanceof Double)
				insns.add(loadDoubleConstant((Double)c));
			else
				insns.add(new LdcInsnNode(c));
			return;
		}

		assert registers.containsKey(v) : v;
		int reg = registers.get(v);
		Type t = v instanceof LocalVariable ? ((LocalVariable)v).getType().getFieldType() : v.getType();
		if (t instanceof ReferenceType || t instanceof NullType)
			insns.add(new VarInsnNode(Opcodes.ALOAD, reg));
		else if (t.equals(longType))
			insns.add(new VarInsnNode(Opcodes.LLOAD, reg));
		else if (t.equals(floatType))
			insns.add(new VarInsnNode(Opcodes.FLOAD, reg));
		else if (t.equals(doubleType))
			insns.add(new VarInsnNode(Opcodes.DLOAD, reg));
		else if (t.isSubtypeOf(intType))
			insns.add(new VarInsnNode(Opcodes.ILOAD, reg));
		else
			throw new AssertionError("unloadable value: "+v);
	}

	private AbstractInsnNode loadIntegerConstant(int c) {
		if (c == -1)
			return new InsnNode(Opcodes.ICONST_M1);
		if (c == 0)
			return new InsnNode(Opcodes.ICONST_0);
		if (c == 1)
			return new InsnNode(Opcodes.ICONST_1);
		if (c == 2)
			return new InsnNode(Opcodes.ICONST_2);
		if (c == 3)
			return new InsnNode(Opcodes.ICONST_3);
		if (c == 4)
			return new InsnNode(Opcodes.ICONST_4);
		if (c == 5)
			return new InsnNode(Opcodes.ICONST_5);
		if (Byte.MIN_VALUE <= c && c <= Byte.MAX_VALUE)
			return new IntInsnNode(Opcodes.BIPUSH, c);
		if (Short.MIN_VALUE <= c && c <= Short.MAX_VALUE)
			return new IntInsnNode(Opcodes.SIPUSH, c);
		return new LdcInsnNode(c);
	}

	private AbstractInsnNode loadLongConstant(long c) {
		if (c == 0)
			return new InsnNode(Opcodes.LCONST_0);
		if (c == 1)
			return new InsnNode(Opcodes.LCONST_1);
		return new LdcInsnNode(c);
	}

	private AbstractInsnNode loadFloatConstant(float c) {
		if (c == 0.0f)
			return new InsnNode(Opcodes.FCONST_0);
		if (c == 1.0f)
			return new InsnNode(Opcodes.FCONST_1);
		if (c == 2.0f)
			return new InsnNode(Opcodes.FCONST_2);
		return new LdcInsnNode(c);
	}

	private AbstractInsnNode loadDoubleConstant(double c) {
		if (c == 0.0)
			return new InsnNode(Opcodes.DCONST_0);
		if (c == 1.0)
			return new InsnNode(Opcodes.DCONST_1);
		return new LdcInsnNode(c);
	}

	private void store(Value v, InsnList insns) {
		assert registers.containsKey(v) : v;
		int reg = registers.get(v);
		Type t = v instanceof LocalVariable ? ((LocalVariable)v).getType().getFieldType() : v.getType();
		if (t instanceof ReferenceType || t instanceof NullType)
			insns.add(new VarInsnNode(Opcodes.ASTORE, reg));
		else if (t.equals(longType))
			insns.add(new VarInsnNode(Opcodes.LSTORE, reg));
		else if (t.equals(floatType))
			insns.add(new VarInsnNode(Opcodes.FSTORE, reg));
		else if (t.equals(doubleType))
			insns.add(new VarInsnNode(Opcodes.DSTORE, reg));
		else if (t.isSubtypeOf(intType))
			insns.add(new VarInsnNode(Opcodes.ISTORE, reg));
		else
			throw new AssertionError("unstorable value: "+v);
	}

	private static String methodDescriptor(Method m) {
		//TODO: maybe put this on Method?  I vaguely recall using it somewhere else...
		MethodType type = m.getType();
		if (m.isConstructor())
			type = type.withReturnType(type.getTypeFactory().getVoidType());
		if (m.hasReceiver())
			type = type.dropFirstArgument();
		return type.getDescriptor();
	}

	private String internalName(Klass k) {
		return k.getName().replace('.', '/');
	}

	/**
	 * Performs peephole optimizations at the bytecode level, primarily to
	 * reduce bytecode size for better inlining.  (HotSpot makes inlining
	 * decisions based partially on the number of bytes in a method.)
	 */
	private void peepholeOptimizations() {
		boolean progress;
		do {
			progress = false;
			progress |= removeLoadStore();
			progress |= removeUnnecessaryGotos();
		} while (progress);
	}

	private static final ImmutableList<Integer> LOADS = ImmutableList.of(
			Opcodes.ALOAD, Opcodes.DLOAD, Opcodes.FLOAD, Opcodes.ILOAD, Opcodes.LLOAD
	);
	private static final ImmutableList<Integer> STORES = ImmutableList.of(
			Opcodes.ASTORE, Opcodes.DSTORE, Opcodes.FSTORE, Opcodes.ISTORE, Opcodes.LSTORE
	);
	/**
	 * Removes "xLOAD N xSTORE N".
	 * @return true iff changes were made
	 */
	private boolean removeLoadStore() {
		InsnList insns = methodNode.instructions;
		for (int i = 0; i < insns.size()-1; ++i) {
			AbstractInsnNode first = insns.get(i);
			int index = LOADS.indexOf(first.getOpcode());
			if (index == -1) continue;
			AbstractInsnNode second = insns.get(i+1);
			if (second.getOpcode() != STORES.get(index)) continue;
			if (((VarInsnNode)first).var != ((VarInsnNode)second).var) continue;
			insns.remove(first);
			insns.remove(second);
			return true;
		}
		return false;
	}

	/**
	 * Removes goto instructions that go to the label immediately following
	 * them.
	 * @return true iff changes were made
	 */
	private boolean removeUnnecessaryGotos() {
		InsnList insns = methodNode.instructions;
		for (int i = 0; i < insns.size()-1; ++i) {
			AbstractInsnNode first = insns.get(i);
			if (first.getOpcode() != Opcodes.GOTO) continue;
			AbstractInsnNode second = insns.get(i+1);
			if (!(second instanceof LabelNode)) continue;
			if (((JumpInsnNode)first).label != second) continue;
			insns.remove(first);
			return true;
		}
		return false;
	}

	public static void main(String[] args) {
		Module m = new Module();
		Klass k = m.getKlass(Module.class);
		Method ar = k.getMethods("getArrayKlass").iterator().next();
		ar.resolve();
		MethodNode mn = unresolve(ar);
		for (int i = 0; i < mn.instructions.size(); ++i) {
			AbstractInsnNode insn = mn.instructions.get(i);
			System.out.format("%d: %d %s%n", i, insn.getOpcode(), insn);
		}
	}
}
