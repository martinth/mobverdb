package de.uzl.mobverdb.join.modes.semi;

import java.rmi.Remote;
import java.rmi.RemoteException;

import de.uzl.mobverdb.join.data.Row;

public interface ISemiJoinClient extends Remote {
    public Row[] joinOn(Integer[] remoteKeys) throws RemoteException;
    public void shutdown() throws RemoteException;
}