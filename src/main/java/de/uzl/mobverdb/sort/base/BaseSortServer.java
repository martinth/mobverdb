package de.uzl.mobverdb.sort.base;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

public abstract class BaseSortServer implements ISortServer, Serializable {

    /** generated */
    private static final long serialVersionUID = 1217951040030975793L;
    /** all clients that have registered themselves */
    protected List<CachingSortClientWrapper> registeredClients;
    /** data that will get sorted */
    protected Collection<String> toBeSorted;
    /** if we are currently sorting */
    protected AtomicBoolean currentlySorting = new AtomicBoolean();

    public BaseSortServer() {
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
    
    @Override
    public void sort() throws RemoteException {
        Preconditions.checkState(this.registeredClients.size() > 0, "their must be at least one client registered to sort");
        Preconditions.checkState(this.toBeSorted.size() > 0, "their must be at least one element to be sorted");
        Preconditions.checkState(!currentlySorting.get(), "sort() was already called");

        currentlySorting.set(true);
        
        this.distributeWork();
    }
    
    @Override
    public Iterator<String> iterator() {
        Preconditions.checkState(currentlySorting.get(), "sort() must be called before iterating");
        return this.getIterator();
    }
    
    protected abstract void distributeWork() throws RemoteException;

    protected abstract Iterator<String> getIterator();



}
