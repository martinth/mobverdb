package de.uzl.mobverdb.join.modes.fetchasneed;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;
import de.uzl.mobverdb.join.modes.JoinPerf;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.utils.Threads;

public class FetchNeeded extends UnicastRemoteObject implements IFetchNeededClient, IFetchNeededServer, MeasurableJoin {
    
    private static final long serialVersionUID = 4715642595903345304L;
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    private static final String BIND_NAME = "fetchAsNeeded";
    private IFetchNeededClient other;
    private Multimap<Integer, Row> keyToData = HashMultimap.create();
    private IFetchNeededServer server;
    private CSVData data;
    private JoinPerf joinPerf = new JoinPerf();
    
    public FetchNeeded(File file, String otherHost) throws NumberFormatException, IOException, NotBoundException {
        super();
        data = new CSVData(file);
        
        if(otherHost != null) {
            for(Row row : data.lines) {
                this.keyToData.put(row.getKey(), row);
            }
            
            server = (IFetchNeededServer) Naming.lookup("//"+otherHost+"/"+FetchNeeded.BIND_NAME);
            server.register(this);
        }
    }
    
    public void join() throws RemoteException, MalformedURLException {
        
        if(server == null) { // this is the host that should join
            Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
            Naming.rebind(BIND_NAME, this);
            
            log.info("Waiting for client to connect");
            while(this.other == null) {
                Threads.trySleep(1000);
            }
            log.info("Client connected. Beginning to join.");
            this.joinPerf.startAll();
            ArrayList<Row> output = new ArrayList<Row>();
            
            for(Row localRow : data.lines) {
                Row[] otherRows = this.other.getRows(localRow.getKey());
                this.joinPerf.rmiCall();
                for(Row otherRow : otherRows) {
                    output.add( new Row(localRow.getKey(), Iterables.concat(localRow.getData(), otherRow.getData())) );
                }
            }
            log.info("Join finished.");
            this.joinPerf.stopAll();
            
            this.other = null;
            try {
                Naming.unbind(BIND_NAME);
            } catch (NotBoundException e) {
                // we ignore this
            }
            UnicastRemoteObject.unexportObject(reg, true);
        } else {
            // do nothing, the other host will call getRows()
        }
    }

    @Override
    public void register(IFetchNeededClient client) throws RemoteException {
        if(this.other == null) {
            this.other = client;
        }
    }

    @Override
    public Row[] getRows(int key) throws RemoteException {
        return Iterables.toArray(this.keyToData.get(key), Row.class);
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf ;
    }

}
