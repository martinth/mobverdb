package de.uzl.mobverdb.sort.base;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * A sort client is responsible for sorting the data that is gets.
 * @author Martin Thurau
 *
 */
public interface ISortClient extends Remote {
	
    /**
     * Puts data to be sorted into the client.
     * @param data data to be sorted
     * @throws RemoteException
     */
	public void putWork(List<String> data) throws RemoteException;
	
	/**
	 * Request a given amount of sorted data from this client. The request will forward 
	 * the internal iterator. So two consecutive with blockSize 2 on the data [2,3,4,5]
	 * will return [2,3] and [4,5]. If there is less data available than requested, the
	 * returned list will be shorter than blockSize. This way the caller can know if 
	 * their is more data to fetch.
	 * @param blockSize the desired size of the to-be-returned list 
	 * @return a list with the (maximal) length of blockSize. The returned list may be
	 * shorter (or empty) if their is less data available than requested 
	 * @throws RemoteException
	 */
	public List<String> getMore(int blockSize) throws RemoteException;

}
