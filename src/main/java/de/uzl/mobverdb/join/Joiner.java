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
        mode.addOption(new Option("w", "ship-whole", false, "Ship-whole join. The optional parameter lets this instance connect o the given host"));
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
            
            /* a local join */
            if(cmd.hasOption("local")) {
                if(cmd.getArgList().size() == 2) {       
                        LocalJoin localJoin = new LocalJoin(new File(cmd.getArgs()[0]), new File(cmd.getArgs()[1]));
                        localJoin.join();    
                } else {
                    throw new ParseException("local mode requires two parameters (<file A> and <file B>");
                }
            }
                
            /* ship whole join */
            if(cmd.hasOption("ship-whole")) {
                String server = cmd.getOptionValue("connect-to");
                if(server != null) {
                    if(cmd.getArgList().size() == 1) {
                        ShipWholeClient client = new ShipWholeClient(new File(cmd.getArgs()[0]), server);
                        
                    } else {
                        throw new ParseException("ship whole mode requires one parameters (<file A>");
                    }
                } else {
                    ShipWholeServer s = new ShipWholeServer();
                    s.serve();
                }
            }   

        } catch (Exception e) {
            log.fatal("Failed with Exception", e);
            System.exit(1);
        }
       
        
    }

}
