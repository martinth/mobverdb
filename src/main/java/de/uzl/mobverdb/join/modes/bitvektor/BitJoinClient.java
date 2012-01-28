package de.uzl.mobverdb.join.modes.bitvektor;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.BitSet;

import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;

public class BitJoinClient extends UnicastRemoteObject implements IBitJoinClient {
    
    private static final long serialVersionUID = 5762321395411810710L;
    private IBitJoinServer server;
    private CSVData data;

    public BitJoinClient(File file, String otherHost) throws NumberFormatException, IOException, NotBoundException {
        super();
        
        data = new CSVData(file);
        
        server = (IBitJoinServer) Naming.lookup("//"+otherHost+"/"+BitJoinServer.BIND_NAME);
        server.register(this);
    }


    @Override
    public Row[] joinOn(BitSet bitSet, UniversalHash hashFunc) throws RemoteException {
        ArrayList<Row> output = new ArrayList<Row>();
        for(Row locaRow : data.lines) {
            if(bitSet.get(hashFunc.hash(locaRow.getKey())) == true) {
                output.add(locaRow);
            }
        }
        return output.toArray(new Row[] {});
    }

    @Override
    public void shutdown() throws RemoteException {
        System.exit(0);
    }
}