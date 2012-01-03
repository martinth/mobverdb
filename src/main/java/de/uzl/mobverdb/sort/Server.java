package de.uzl.mobverdb.sort;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.base.Preconditions;

import com.google.common.collect.Iterables;

/**
 * Implements a distributes merge sort.
 * @author Martin Thurau
 *
 */
public class Server implements ISortServer, Serializable {
	
	/** generated */
    private static final long serialVersionUID = 3753586170293486054L;
    /** all clients that have registered themselves */
	private List<CachingSortClientWrapper> registeredClients;
	/** data that will get sorted */
	private Collection<String> toBeSorted;
	/** if we are currently sorting */
	private AtomicBoolean currentlySorting = new AtomicBoolean();
	
	public Server() {
		this.registeredClients = new ArrayList<CachingSortClientWrapper>();
		this.toBeSorted = new ArrayList<String>();
	}

	public boolean registerClient(ISortClient client) throws RemoteException {
		if(currentlySorting.get()) {
			return false;
		} else {
			this.registeredClients.add(new CachingSortClientWrapper(client, 10));
			return true;
		} 
	}

	public void add(String element) {
		Preconditions.checkState(!currentlySorting.get(), "You cannot add elements after calling sort()");
		this.toBeSorted.add(element);
	}

	public void sort() throws RemoteException {
		Preconditions.checkState(this.registeredClients.size() > 0, "their must be at least one client registered to sort");
		Preconditions.checkState(this.toBeSorted.size() > 0, "their must be at least one element to be sorted");
		Preconditions.checkState(!currentlySorting.get(), "sort() was already called");

		currentlySorting.set(true);
		 
		int sliceSize = (int) Math.ceil(((float)this.toBeSorted.size()) / this.registeredClients.size());
		Iterator<CachingSortClientWrapper> clientsToBeUsed = this.registeredClients.iterator();
		for(List<String> subList : Iterables.partition(this.toBeSorted, sliceSize)) {
			CachingSortClientWrapper client = clientsToBeUsed.next();
			client.putWork(subList); //FIXME ich bin mir noch nicht schlüssig was wir im Fehlerfall machen
		}
	}

	public Iterator<String> iterator() {
		Preconditions.checkState(currentlySorting.get(), "sort() must be called before iterating");
		
		
		return new Iterator<String>() {

			public String next() {
				if(!hasNext())  throw new NoSuchElementException();
				
				/* find the next element */
				CachingSortClientWrapper min = null;
			
				try {
					for (CachingSortClientWrapper current : registeredClients) {
						
				
						if(min == null || min.peekNext() == null || (current.peekNext() != null && min.peekNext().compareTo(current.peekNext()) > 0)) {
							min = current;
						}
						
						
					}
	
					return min.getNext();
				} catch (RemoteException e) {
					/* this is a stupid idea, but since we MUST implement an Interator we must
					 * somehow deal with a RemoteException and this place is as bad as any other. It
					 * would have been a better to idea to not use an iterator at all. */
					throw new NoSuchElementException("Internal error. RemoteException occured: "+e);
				}
				
			}
			
			public boolean hasNext() {
				// as long as there is one unfinished client we have data
				for (CachingSortClientWrapper client : registeredClients) {
					try {
                        if(!client.isFinished()) {
                        	return true;
                        }
                    } catch (RemoteException e) {
                        
                    }
				}
				return false;
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
		
	}


}
