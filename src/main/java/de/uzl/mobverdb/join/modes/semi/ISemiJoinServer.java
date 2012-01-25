package de.uzl.mobverdb.join.modes.semi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ISemiJoinServer extends Remote {
    public void register(ISemiJoinClient client) throws RemoteException;
}