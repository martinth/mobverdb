package de.uzl.mobverdb.join.modes.shipwhole;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IShipWholeServer extends Remote {
    public void register(IShipWholeClient client) throws RemoteException;
}