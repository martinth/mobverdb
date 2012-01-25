package de.uzl.mobverdb.join.modes.bitvektor;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IBitJoinServer extends Remote {
    public void register(IBitJoinClient client) throws RemoteException;
}