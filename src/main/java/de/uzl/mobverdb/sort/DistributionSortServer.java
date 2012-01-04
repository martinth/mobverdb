package de.uzl.mobverdb.sort;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;

import de.uzl.mobverdb.sort.base.BaseSortServer;

/**
 * Implements a distributes merge sort.
 * @author Martin Thurau
 *
 */
public class DistributionSortServer extends BaseSortServer  {
        
    /** generated */
    private static final long serialVersionUID = 3753586170293486054L;

	@Override
    protected void distributeWork() throws RemoteException {
	    Collections.shuffle(toBeSorted);
	    
	    int clientCount = registeredClients.size();
	    
	    String[] pivots = new String[clientCount];
	    List<List<String>> buckets = new ArrayList<List<String>>();
	    
	    for(int i = 0; i < clientCount; i++) {
	        pivots[i] = toBeSorted.get(i);
	        buckets.add(new ArrayList<String>());
	    }
	    Arrays.sort(pivots);
	    
	    
	    for(String element : toBeSorted) {
	        for(int i = 0; i < clientCount; i++) {
	            if(pivots[i].compareTo(element) > 0) {
	                buckets.get(i).add(element);
	                break;
	            }
	        }
	    }
	    
	    for(int i = 0; i < clientCount; i++) {
	        //System.out.printf("Bucket %d: %d\n", i, buckets.get(i).size());
	        registeredClients.get(i).putWork(buckets.get(i));
	    }

	}

	@Override
    protected Iterator<String> getIterator() {
		Preconditions.checkState(currentlySorting.get(), "sort() must be called before iterating");
		
		return new Iterator<String>() {
		    
		    int curClient = 0;

			public String next() {
				if(!hasNext())  throw new NoSuchElementException();
				try {
				    return registeredClients.get(curClient).getNext();
				} catch (RemoteException e) {
					/* this is a stupid idea, but since we MUST implement an Interator we must
					 * somehow deal with a RemoteException and this place is as bad as any other. It
					 * would have been a better to idea to not use an iterator at all. */
					throw new NoSuchElementException("Internal error. RemoteException occured: "+e);
				}
				
			}
			
			public boolean hasNext() {
			    
			    try {
                    if(registeredClients.get(curClient).isFinished()) {
                        if(curClient >= registeredClients.size()-1) {
                            return false;
                        } else {
                            curClient++;
                            return this.hasNext();
                        }
                       
                    } else {
                        return true;
                    }
                } catch (RemoteException e) {
                    return false;
                }

			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
		
	}


}
