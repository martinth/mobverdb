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

import com.google.common.collect.Iterables;
import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;
import de.uzl.mobverdb.join.modes.JoinPerf;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.utils.Threads;

public class FetchNeededServer extends UnicastRemoteObject implements IFetchNeededServer, MeasurableJoin {
    
    private static final long serialVersionUID = 4715642595903345304L;
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    public static final String BIND_NAME = "fetchAsNeeded";
    private JoinPerf joinPerf = new JoinPerf(BIND_NAME);

    private IFetchNeededClient other;
    private CSVData data;
    
    public FetchNeededServer(File file) throws NumberFormatException, IOException, NotBoundException {
        super();
        data = new CSVData(file);
    }
    
    public void join() throws RemoteException, MalformedURLException {
        
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind(BIND_NAME, this);
        
        log.info("Waiting for client to connect");
        while(this.other == null) {
            Threads.trySleep(1000);
        }
        log.info("Client connected. Beginning to join.");
        ArrayList<Row> output = new ArrayList<Row>();
        
        joinPerf.totalTime.start();
        joinPerf.localJoinTime.start();
        for(Row localRow : data.lines) {
            Row[] otherRows = this.other.getRows(localRow.getKey());
            this.joinPerf.rmiCall();
            for(Row otherRow : otherRows) {
                output.add( new Row(localRow.getKey(), Iterables.concat(localRow.getData(), otherRow.getData())) );
            }
        }
        joinPerf.localJoinTime.stop();
        joinPerf.totalTime.stop();
        
        log.info("Join finished.");
                
        try {
            this.other.shutdown();
            Threads.trySleep(1000);
            Naming.unbind(BIND_NAME);
            UnicastRemoteObject.unexportObject(reg, true);
        } catch(Exception e) {
            // ignore this, we will exit anyway
        }
    }

    @Override
    public void register(IFetchNeededClient client) throws RemoteException {
        if(this.other == null) {
            this.other = client;
        }
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf ;
    }

}
