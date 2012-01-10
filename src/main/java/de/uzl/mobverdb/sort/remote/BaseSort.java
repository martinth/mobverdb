package de.uzl.mobverdb.sort.remote;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

import de.uzl.mobverdb.sort.remote.interfaces.ISortClient;
import de.uzl.mobverdb.sort.remote.interfaces.ISortServer;

public abstract class BaseSort extends UnicastRemoteObject implements ISortServer {
    
    protected BaseSort() throws RemoteException {
        super();
        // TODO Auto-generated constructor stub
    }

    /** generated */
    private static final long serialVersionUID = 1217951040030975793L;
    /** all clients that have registered themselves */
    protected List<CachingSortClientWrapper> registeredClients = new ArrayList<CachingSortClientWrapper>();
    /** data that will get sorted */
    protected List<String> toBeSorted = new ArrayList<String>();
    /** if we are currently sorting */
    protected AtomicBoolean currentlySorting = new AtomicBoolean();

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
    
    public int getClientCount() {
        return this.registeredClients.size();
    }
    
    
    protected abstract void distributeWork() throws RemoteException;

    protected abstract Iterator<String> getIterator();

}
