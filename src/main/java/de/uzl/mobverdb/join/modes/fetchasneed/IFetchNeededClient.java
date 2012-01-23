package de.uzl.mobverdb.join.modes.fetchasneed;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import de.uzl.mobverdb.join.data.Row;

public interface IFetchNeededClient extends Remote {
    public Row[] getRows(int key) throws RemoteException;
}