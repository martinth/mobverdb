package de.uzl.mobverdb.join;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import de.uzl.mobverdb.join.modes.LocalJoin;
import de.uzl.mobverdb.join.modes.MeasurableJoin;
import de.uzl.mobverdb.join.modes.fetchasneed.FetchNeededClient;
import de.uzl.mobverdb.join.modes.fetchasneed.FetchNeededServer;
import de.uzl.mobverdb.join.modes.semi.SemiJoinClient;
import de.uzl.mobverdb.join.modes.semi.SemiJoinServer;
import de.uzl.mobverdb.join.modes.shipwhole.ShipWholeClient;
import de.uzl.mobverdb.join.modes.shipwhole.ShipWholeServer;

public class Joiner {
    
    private final static Logger log = Logger.getLogger(Joiner.class.getCanonicalName());

    /**
     * @param args
     */
    public static void main(String[] args) {
        Options options = new Options();
        
        // option group for join type
        OptionGroup mode = new OptionGroup();
        mode.addOption(new Option("l", "local", false, "Do a local join"));
        mode.addOption(new Option("w", "ship-whole", false, "Ship-whole join"));
        mode.addOption(new Option("f", "fetch-needed", false, "Fetch-as-needed join"));
        mode.addOption(new Option("s", "semi", false, "Semi join"));
        options.addOptionGroup(mode);
        
        options.addOption("c", "connect-to", true, "in client/server mode: where to connect to");
        
        //options.addOption(new Option("b", "blocksize", true, "Blocksize for data fetching from client (default: 10)"));
        CommandLineParser parser = new PosixParser();
        try {
            setupAndRun(parser.parse(options, args));
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(e.getMessage()+"\n");
            formatter.printHelp("joiner.jar [[file A [file B]] ", options);
        }
    }

    private static void setupAndRun(CommandLine cmd) throws ParseException {
        
        try {
            MeasurableJoin measuredJoin = null;
            
            
            /* a local join */
            if(cmd.hasOption("local")) {
                if(cmd.getArgList().size() == 2) {       
                        LocalJoin localJoin = new LocalJoin(new File(cmd.getArgs()[0]), new File(cmd.getArgs()[1]));
                        localJoin.join();
                        measuredJoin = localJoin;
                } else {
                    throw new ParseException("local mode requires two parameters (<file A> and <file B>");
                }
            }
                
            /* ship whole join */
            if(cmd.hasOption("ship-whole")) {
                String server = cmd.getOptionValue("connect-to");
                if(server != null) {
                    if(cmd.getArgList().size() == 1) {
                        new ShipWholeClient(new File(cmd.getArgs()[0]), server);
                    } else {
                        throw new ParseException("ship whole mode requires one parameters (<file A>");
                    }
                } else {
                    ShipWholeServer s = new ShipWholeServer();
                    s.serve();
                    measuredJoin = s;
                }
            }   
            
            /* fetch as needed join */
            if(cmd.hasOption("fetch-needed")) {
                if(cmd.getArgList().size() == 1) {
                    String server = cmd.getOptionValue("connect-to");
                    if(server != null) {
                        new FetchNeededClient(new File(cmd.getArgs()[0]), server);
                    } else {
                        FetchNeededServer serverInstance = new FetchNeededServer(new File(cmd.getArgs()[0]));
                        serverInstance.join();
                        measuredJoin = serverInstance;
                    }
                    
                    
                } else {
                    throw new ParseException("ship whole mode requires one parameters (<file A>");
                }
            } 
            
            /* semi join */
            if(cmd.hasOption("semi")) {
                if(cmd.getArgList().size() == 1) {
                    String server = cmd.getOptionValue("connect-to");
                    if(server != null) {
                        new SemiJoinClient(new File(cmd.getArgs()[0]), server);
                    } else {
                        SemiJoinServer serverInstance = new SemiJoinServer(new File(cmd.getArgs()[0]));
                        serverInstance.join();
                        measuredJoin = serverInstance;
                    }
                    
                    
                } else {
                    throw new ParseException("ship whole mode requires one parameters (<file A>");
                }
            }
            
            if(measuredJoin != null) {
                System.out.println(measuredJoin.getPerf());
                System.exit(0);
            }

        } catch (Exception e) {
            log.fatal("Failed with Exception", e);
            System.exit(1);
        }
       
        
    }

}
