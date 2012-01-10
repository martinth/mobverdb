package de.uzl.mobverdb.sort.remote;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import de.uzl.mobverdb.sort.remote.interfaces.ISortClient;

public class Client extends UnicastRemoteObject implements ISortClient {
    
    public Client() throws RemoteException {
        super();
        // TODO Auto-generated constructor stub
    }

    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    
	/** generated */
    private static final long serialVersionUID = -4469350325989245038L;
    
	private List<String> toBeSorted;
	private Iterator<String> iter;
	
    private FutureTask<Iterator<String>> sorterTask;
    private ExecutorService executor = Executors.newFixedThreadPool(1);


	/**
	 * {@inheritDoc}
	 */
	public void putWork(List<String> data) throws RemoteException {
		toBeSorted = Lists.newArrayList(data);
		log.debug("Got data to be sorted");
		
		/* the actual sorting will be done in a different thread so that this
		 * method can return as fast as possible */
		sorterTask = new FutureTask<Iterator<String>>(new Callable<Iterator<String>>() {
            @Override
            public Iterator<String> call() throws Exception {
                Collections.sort(toBeSorted);
                log.debug("Sorter task finished sorting");
                return toBeSorted.iterator();
            }
        });
		executor.execute(sorterTask);
		log.debug("Created and executed sorter task");
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
            log.debug(String.format("Got request for %d items. Returning %d", blockSize, ret.size()));
            return ret;
        } catch (InterruptedException e) {
            throw new RemoteException("Sorting was interrupted.", e);
        } catch (ExecutionException e) {
            throw new RemoteException("An exception occured in the sorting task.", e);
        }
	}
	
	public boolean isFinished() {
	    try {
            return sorterTask != null && sorterTask.isDone() && !sorterTask.get().hasNext();
        } catch (InterruptedException e) {
            return true;
        } catch (ExecutionException e) {
            return true;
        }
	}

}
