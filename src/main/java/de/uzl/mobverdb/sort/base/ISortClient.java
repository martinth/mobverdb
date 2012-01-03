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
	
	public void putWork(List<String> data) throws RemoteException;
	
	public List<String> getMore(int blockSize) throws RemoteException;

}
