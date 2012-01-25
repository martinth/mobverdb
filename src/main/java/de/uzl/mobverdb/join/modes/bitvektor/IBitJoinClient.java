package de.uzl.mobverdb.join.modes.bitvektor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.BitSet;

import de.uzl.mobverdb.join.data.Row;

public interface IBitJoinClient extends Remote {
    public Row[] joinOn(BitSet bitSet, UniversalHash hashFunc)throws RemoteException;
}