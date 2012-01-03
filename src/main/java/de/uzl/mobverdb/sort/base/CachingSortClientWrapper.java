package de.uzl.mobverdb.sort.base;

import java.rmi.RemoteException;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

/**
 * A Wrapper class for {@link ISortClient} that hides a blocked fetch below a simple 
 * iterator-like interface.
 * @author Martin Thurau
 *
 */
public class CachingSortClientWrapper { 

	/** the wrapped ISortClient */
	private ISortClient actualClient;
	/** internal cache for data */
	private Queue<String> dataCache = new LinkedList<String>();
	/** how large the size of the blocks that get fetched from the client is */
	private int cacheSize;
	/** indicator if the client is empty */
	private boolean clientEmpty = false;

	public CachingSortClientWrapper(ISortClient actualClient, int cacheSize) {
		this.actualClient = actualClient;
		this.cacheSize = cacheSize;
	}

	/**
	 * Put data to the wrapped client.
	 * @param data 
	 * @throws RemoteException
	 */
	public void putWork(List<String> data) throws RemoteException {
		this.actualClient.putWork(data);
	}

	/**
	 * @return the first item from the client but does not remove it 
	 * @throws RemoteException
	 */
	public String peekNext() throws RemoteException {
		refillIfNeeded();
		return dataCache.peek();
	}

	/**
	 * @return returns and remove the first item from the client
	 * @throws RemoteException
	 */
	public String getNext() throws RemoteException {
		refillIfNeeded();
		return dataCache.poll();
	}
	
	public boolean isFinished() throws RemoteException {
	    refillIfNeeded();
		return dataCache.isEmpty() && this.clientEmpty;
	}
	
	/**
	 * Refills the local cache (if needed) and sets clientEmpty
	 * @throws RemoteException
	 */
	private void refillIfNeeded() throws RemoteException {
		// if the client is known as empty, don't do anything
		if(clientEmpty) {
			return;
		// if the cache is empty, fetch data
		} else if(this.dataCache.isEmpty()) {
			List<String> fromClient = actualClient.getMore(cacheSize);
			this.dataCache.addAll(fromClient);
			// if the client returns less data than request, it has not more data
			if(fromClient.size() < cacheSize) {
				this.clientEmpty = true;
			}
		}
	
	}
	
	public String toString() {
		return Objects.toStringHelper(this)
				.add("actualclient", actualClient)
				.add("clientEmpty", clientEmpty)
				.add("dataCache", Joiner.on(",").join(dataCache))
				.toString();
	}

}
