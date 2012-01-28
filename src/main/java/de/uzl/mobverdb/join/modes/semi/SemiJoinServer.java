package de.uzl.mobverdb.join.modes.semi;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import de.uzl.mobverdb.join.JoinUtils;
import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;
import de.uzl.mobverdb.join.modes.JoinPerf;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.utils.Threads;

public class SemiJoinServer extends UnicastRemoteObject implements ISemiJoinServer, MeasurableJoin {

    private static final long serialVersionUID = -5035845909042215587L;
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    public static final String BIND_NAME = "semiJoin";
    private JoinPerf joinPerf = new JoinPerf(BIND_NAME);

    private ISemiJoinClient client;
    private CSVData data;
    
    public SemiJoinServer(File file) throws NumberFormatException, IOException {
        super();
        this.data = new CSVData(file);
    }
    
    public void join() throws RemoteException, MalformedURLException {
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind(BIND_NAME, this);
        
        log.info("Waiting for client to connect");
        while(client == null) {
            Threads.trySleep(1000);
        }
        log.info("Client connected. Beginning to join.");
        joinPerf.totalTime.start();
        
        log.info("Creating projection of local data");
        joinPerf.prepareTime.start();
        Set<Integer> localKeys = new HashSet<Integer>();
        for(Row r : data.lines) {
            localKeys.add(r.getKey());
        }
        joinPerf.prepareTime.stop();
        
        joinPerf.remoteJoinTime.start();
        Row[] remoteJoinedData = client.joinOn(localKeys.toArray(new Integer[] {}));
        joinPerf.rmiCall();
        joinPerf.remoteJoinTime.stop();
        
        joinPerf.localJoinTime.start();
        Row[] joinedData = JoinUtils.nestedLoopJoin(data.lines, remoteJoinedData);
        joinPerf.localJoinTime.stop();
        joinPerf.totalTime.stop();
        
        try {
            this.client.shutdown();
            Threads.trySleep(1000);
            Naming.unbind(BIND_NAME);
            UnicastRemoteObject.unexportObject(reg, true);
        } catch(Exception e) {
            // ignore this, we will exit anyway
        }
    }

    @Override
    public void register(ISemiJoinClient client) throws RemoteException {
        if(this.client == null) {
            this.client = client;
        }
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf;
    }
    
}


