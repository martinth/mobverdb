package de.uzl.mobverdb.join.modes.shipwhole;

import java.rmi.Remote;
import java.rmi.RemoteException;

import de.uzl.mobverdb.join.data.Row;

public interface IShipWholeClient extends Remote {
    public Row[] getData() throws RemoteException;
}