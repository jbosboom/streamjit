package edu.mit.streamjit.impl.blob;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.util.CollectionUtils;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DrainData represents the state of a Blob after it has drained: any data left
 * in the edges between workers and the state of any stateful workers.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 7/22/2013
 */
public class DrainData implements Serializable {
	private static final long serialVersionUID = 1L;
	private ImmutableMap<Token, ImmutableList<Object>> data;
	private transient ImmutableTable<Integer, String, Object> state;
	//TODO: in-flight messages

	public DrainData(Map<Token, ? extends List<Object>> data, Table<Integer, String, Object> state) {
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap.builder();
		for (Map.Entry<Token, ? extends List<Object>> e : data.entrySet())
			dataBuilder.put(e.getKey(), ImmutableList.copyOf(e.getValue()));
		this.data = dataBuilder.build();
		this.state = ImmutableTable.copyOf(state);
	}

	public ImmutableList<Object> getData(Token token) {
		return data.get(token);
	}

	public ImmutableMap<String, Object> getWorkerState(int workerId) {
		return state.row(workerId);
	}

	public Object getWorkerState(int workerId, String fieldName) {
		return state.get(workerId, fieldName);
	}

	/**
	 * Merge this DrainData with the given DrainData object, appending any
	 * channel data to the data in this DrainData (just as adding to a List
	 * appends at the end).
	 * @param other the DrainData to merge with
	 * @return a merged DrainData
	 */
	public DrainData merge(DrainData other) {
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap.builder();
		for (Token t : Sets.union(data.keySet(), other.data.keySet())) {
			ImmutableList<Object> us = getData(t) != null ? getData(t) : ImmutableList.of();
			ImmutableList<Object> them = other.getData(t) != null ? other.getData(t) : ImmutableList.of();
			dataBuilder.put(t, ImmutableList.builder().addAll(us).addAll(them).build());
		}

		if (!Sets.intersection(state.rowKeySet(), other.state.rowKeySet()).isEmpty())
			throw new IllegalArgumentException("bad merge: one worker's state split across DrainData");
		return new DrainData(dataBuilder.build(), CollectionUtils.union(state, other.state));
	}

	/**
	 * Returns a subset of this DrainData limited to the given set of worker
	 * identifiers.  The returned set contains data for every token containing
	 * down stream worker identifier and state for all fields for each of the
	 * given identifiers.
	 * @param workerIds the set of worker identifiers
	 * @return a subset of this DrainData limited to the given set of
	 * identifiers
	 */
	public DrainData subset(Set<Integer> workerIds) {
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap.builder();
		for (Map.Entry<Token, ? extends List<Object>> e : data.entrySet())
			if (workerIds.contains(e.getKey().getDownstreamIdentifier()))
				dataBuilder.put(e.getKey(), ImmutableList.copyOf(e.getValue()));

		ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable.builder();
		for (Table.Cell<Integer, String, Object> c : state.cellSet())
			if (workerIds.contains(c.getRowKey()))
				stateBuilder.put(c);
		return new DrainData(dataBuilder.build(), stateBuilder.build());
	}

	@Override
	public String toString() {
		return String.format("[%s, %s]", data, state);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		oos.writeObject(state.rowMap());
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		ImmutableMap<Integer, Map<String, Object>> map = (ImmutableMap<Integer, Map<String, Object>>) ois.readObject();
		ImmutableTable.Builder<Integer, String, Object> builder = ImmutableTable.builder();
		for (Map.Entry<Integer, Map<String, Object>> e1 : map.entrySet())
			for (Map.Entry<String, Object> e2 : e1.getValue().entrySet())
				builder.put(e1.getKey(), e2.getKey(), e2.getValue());
		state = builder.build();
	}
}
