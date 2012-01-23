package de.uzl.mobverdb.join.modes.fetchasneed;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;

public class FetchNeededClient extends UnicastRemoteObject implements IFetchNeededClient {
    
    private static final long serialVersionUID = 4712432535913345304L;
    private static final String BIND_NAME = "fetchAsNeeded";
    private Multimap<Integer, Row> keyToData = HashMultimap.create();
    private IFetchNeededServer server;
    private CSVData data;
    
    public FetchNeededClient(File file, String otherHost) throws NumberFormatException, IOException, NotBoundException {
        super();
        data = new CSVData(file);
        
        for(Row row : data.lines) {
            this.keyToData.put(row.getKey(), row);
        }
        
        server = (IFetchNeededServer) Naming.lookup("//"+otherHost+"/"+FetchNeededClient.BIND_NAME);
        server.register(this);
    }
    

    @Override
    public Row[] getRows(int key) throws RemoteException {
        return Iterables.toArray(this.keyToData.get(key), Row.class);
    }

}
