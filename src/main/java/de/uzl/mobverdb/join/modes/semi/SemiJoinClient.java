package de.uzl.mobverdb.join.modes.semi;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;

public class SemiJoinClient extends UnicastRemoteObject implements ISemiJoinClient {
    
    private static final long serialVersionUID = 6120819029460895901L;
    private Multimap<Integer, Row> keyToData = HashMultimap.create();
    private ISemiJoinServer server;

    public SemiJoinClient(File file, String otherHost) throws NumberFormatException, IOException, NotBoundException {
        super();
        
        for(Row row : new CSVData(file).lines) {
            this.keyToData.put(row.getKey(), row);
        }
        
        server = (ISemiJoinServer) Naming.lookup("//"+otherHost+"/"+SemiJoinServer.BIND_NAME);
        server.register(this);
    }

    @Override
    public Row[] joinOn(Integer[] remoteKeys) {
        ArrayList<Row> output = new ArrayList<Row>();
        for(Integer remoteKey : remoteKeys) {
            for(Row localRow : keyToData.get(remoteKey)) {
                output.add(localRow);
            }
        }
        return output.toArray(new Row[] {});
    }

    @Override
    public void shutdown() throws RemoteException {
        System.exit(0);
    }
}