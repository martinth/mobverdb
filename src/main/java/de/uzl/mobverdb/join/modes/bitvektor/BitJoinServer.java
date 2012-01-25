package de.uzl.mobverdb.join.modes.bitvektor;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import de.uzl.mobverdb.join.JoinUtils;
import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;
import de.uzl.mobverdb.join.modes.JoinPerf;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.utils.Threads;

public class BitJoinServer extends UnicastRemoteObject implements IBitJoinServer, MeasurableJoin {

    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    public static final String BIND_NAME = "semiJoin";
    private JoinPerf joinPerf = new JoinPerf();

    private IBitJoinClient client;
    private CSVData data;
    private int vektorSize;
    
    public BitJoinServer(File file) throws NumberFormatException, IOException {
        this(file, 10);
    }
    
    public BitJoinServer(File file, int vektorSize) throws NumberFormatException, IOException {
        super();
        this.data = new CSVData(file);
        this.vektorSize = vektorSize;
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
        
        log.info("Creating bitvektor of local data");
        BitSet bitSet = new BitSet(vektorSize);
        UniversalHash hashFunc = new UniversalHash(vektorSize);
        for(Row r : data.lines) {
            bitSet.set(hashFunc.hash(r.getKey()), true);
        }
        
        log.info("Fetching from client");
        Row[] remoteJoinedData = client.joinOn(bitSet, hashFunc);
        joinPerf.rmiCall();
        
        joinPerf.joinTime.start();
        log.info("Doing Local join");
        Row[] joinedData = JoinUtils.nestedLoopJoin(data.lines, remoteJoinedData);
        joinPerf.stopAll();
        
        client = null;
        try {
            Naming.unbind(BIND_NAME);
        } catch (NotBoundException e) {
            // we ignore this
        }
        UnicastRemoteObject.unexportObject(reg, true);
    }

    @Override
    public void register(IBitJoinClient client) throws RemoteException {
        if(this.client == null) {
            this.client = client;
        }
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf;
    }
    
}


