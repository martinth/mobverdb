package de.uzl.mobverdb.join.modes.shipwhole;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import de.uzl.mobverdb.join.JoinUtils;
import de.uzl.mobverdb.join.data.Row;
import de.uzl.mobverdb.join.modes.JoinPerf;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.mobverdb.join.modes.semi.ISemiJoinClient;
import de.uzl.utils.Threads;

public class ShipWholeServer extends UnicastRemoteObject implements IShipWholeServer, MeasurableJoin {
    
    private static final long serialVersionUID = -5430396965086442250L;
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    public final static String BIND_NAME = "shipWholeJoin"; 
    private JoinPerf joinPerf = new JoinPerf(BIND_NAME);

    private ArrayList<IShipWholeClient> clients = new ArrayList<IShipWholeClient>();
    Row[] output;
    
    public ShipWholeServer() throws RemoteException {
        super();
    }

    public void serve() throws RemoteException, MalformedURLException {
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind(BIND_NAME, this);
        
        log.info("Server bound, waiting for clients");
        
        while(clients.size() < 2) {
            Threads.trySleep(1000);
        }
        log.info("Two clients connect, fetching their data");
        joinPerf.totalTime.start();
        
        Row[] dataR = this.clients.get(0).getData();
        Row[] dataS = this.clients.get(1).getData();
        this.joinPerf.rmiCalls(2);
        
        log.info("Data fetched, beginning to join");
        joinPerf.localJoinTime.start();
        output = JoinUtils.nestedLoopJoin(dataR, dataS);
        joinPerf.localJoinTime.stop();
        joinPerf.totalTime.stop();
        
        log.info("Join completed");
        
        for(IShipWholeClient client : clients) {
            try {
                client.shutdown();
            } catch(Exception e) {
                // ignore this, we will exit anyway
            }
        }
        
        try {
            Naming.unbind(BIND_NAME);
            UnicastRemoteObject.unexportObject(reg, true);
        } catch(Exception e) {
            // ignore this, we will exit anyway
        }
    }
    
    public Row[] getResults() {
        return this.output;
    }

    @Override
    public void register(IShipWholeClient client) throws RemoteException {
        this.clients.add(client);
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf;
    }
    
}