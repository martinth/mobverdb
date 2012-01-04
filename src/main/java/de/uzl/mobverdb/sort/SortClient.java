package de.uzl.mobverdb.sort;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import com.google.common.collect.Lists;

import de.uzl.mobverdb.sort.base.ISortClient;

public class SortClient implements ISortClient, Serializable {
	
	/** generated */
    private static final long serialVersionUID = -4469350325989245038L;
    
	private List<String> toBeSorted;
	private Iterator<String> iter;
	
    private FutureTask<Iterator<String>> sorterTask;
    ExecutorService executor = Executors.newFixedThreadPool(1);
    

	/**
	 * {@inheritDoc}
	 */
	public void putWork(List<String> data) throws RemoteException {
		toBeSorted = Lists.newArrayList(data);
		
		/* the actual sorting will be done in a different thread so that this
		 * method can return as fast as possible */
		sorterTask = new FutureTask<Iterator<String>>(new Callable<Iterator<String>>() {
            @Override
            public Iterator<String> call() throws Exception {
                Collections.sort(toBeSorted);
                return toBeSorted.iterator();
            }
        });
		executor.execute(sorterTask);
		
	}
	
	/**
	 * {@inheritDoc}
	 */
	public List<String> getMore(int blockSize) throws RemoteException {
		LinkedList<String> ret = new LinkedList<String>();
		
		try {
		    /* the call to get() will block until the sorting is done */
            iter = sorterTask.get();
            
            /* Get as many elements as requested from the result list by using the 
             * iterator. If the end of the list is reached we return less than requested
             * so the requester can know there is no more data */ 
            for(int i = 0; i < blockSize; i++) {
            	if(iter.hasNext()) {
            		ret.add(iter.next());
            	} else {
            		break;
            	}
            }
            return ret;
        } catch (InterruptedException e) {
            throw new RemoteException("Sorting was interrupted.", e);
        } catch (ExecutionException e) {
            throw new RemoteException("An exception occured in the sorting task.", e);
        }
	}

}
