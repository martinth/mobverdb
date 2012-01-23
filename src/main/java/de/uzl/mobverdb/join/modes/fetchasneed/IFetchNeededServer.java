package de.uzl.mobverdb.join.modes.fetchasneed;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IFetchNeededServer extends Remote {
    public void register(IFetchNeededClient client) throws RemoteException;
}