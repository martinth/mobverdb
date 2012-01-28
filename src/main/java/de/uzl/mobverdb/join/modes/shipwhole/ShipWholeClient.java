package de.uzl.mobverdb.join.modes.shipwhole;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;

public class ShipWholeClient extends UnicastRemoteObject implements IShipWholeClient  {

    private static final long serialVersionUID = -3317114769299242224L;
    private CSVData data;
    private IShipWholeServer other;
    
    public ShipWholeClient(File file, String otherHost) throws NumberFormatException, IOException, NotBoundException {
        this.data = new CSVData(file);
        this.other = (IShipWholeServer) Naming.lookup("//"+otherHost+"/"+ShipWholeServer.BIND_NAME);
        this.other.register(this);
    }
    
    @Override
    public Row[] getData() throws RemoteException {
        return this.data.lines;
    }

    @Override
    public void shutdown() throws RemoteException {
        System.exit(0);
    }
}