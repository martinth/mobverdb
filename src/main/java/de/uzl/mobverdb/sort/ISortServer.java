package de.uzl.mobverdb.sort;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Iterator;

/**
 * A sort server provides two kinds of methods: remote methods, that should be called by sort clients
 * and local methods, that provide access to the sort functionality.
 * @author Martin Thurau
 *
 */
public interface ISortServer extends Remote, Iterable<String>  {
	
	/**
	 * Register a new sort client. Clients can only register successfully if sort()
	 * has not been called.
	 * @param client client to register
	 * @return if the client was registered
	 * @throws RemoteException
	 */
	public boolean registerClient(ISortClient client) throws RemoteException;
	
	/**
	 * Add a new item to the "to be sorted" data
	 * @param element
	 */
	public abstract void add(String element);

	/**
	 * Initiates the sort operation
	 * @throws RemoteException 
	 */
	public abstract void sort() throws RemoteException;
	
	public Iterator<String> iterator();


}
