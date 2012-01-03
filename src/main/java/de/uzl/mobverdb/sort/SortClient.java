package de.uzl.mobverdb.sort;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import de.uzl.mobverdb.sort.base.ISortClient;

public class SortClient implements ISortClient, Serializable {
	
	/** generated */
    private static final long serialVersionUID = -4469350325989245038L;
    private Iterator<String> iter;
	private List<String> sorted;

	public void putWork(List<String> data) throws RemoteException {
		sorted = new ArrayList<String>(data);
		Collections.sort(sorted);
		this.iter = sorted.iterator();
	}

	public List<String> getMore(int blockSize) throws RemoteException {
		LinkedList<String> ret = new LinkedList<String>();
		for(int i = 0; i < blockSize; i++) {
			if(iter.hasNext()) {
				ret.add(iter.next());
			} else {
				break;
			}
		}
		return ret;
	}

}
