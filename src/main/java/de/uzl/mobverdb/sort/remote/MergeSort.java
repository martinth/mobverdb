package de.uzl.mobverdb.sort.remote;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import com.google.common.base.Preconditions;

import com.google.common.collect.Iterables;


/**
 * Implements a distributes merge sort.
 * @author Martin Thurau
 *
 */
public class MergeSort extends BaseSort  {
    
    public MergeSort() throws RemoteException {
        super();
        // TODO Auto-generated constructor stub
    }

    /** generated */
    private static final long serialVersionUID = 3753586170293486054L;

	@Override
    protected void distributeWork() throws RemoteException {
		int sliceSize = (int) Math.ceil(((float)this.toBeSorted.size()) / this.registeredClients.size());
		Iterator<CachingSortClientWrapper> clientsToBeUsed = this.registeredClients.iterator();
		for(List<String> subList : Iterables.partition(this.toBeSorted, sliceSize)) {
		    ArrayList<String> list = new ArrayList<String>(subList);
			CachingSortClientWrapper client = clientsToBeUsed.next();
			client.putWork(list); //FIXME ich bin mir noch nicht schlüssig was wir im Fehlerfall machen
		}
	}

	@Override
    protected Iterator<String> getIterator() {
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
