package org.apache.usergrid.persistence.core.cassandra;


import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;

import org.apache.usergrid.persistence.core.util.AvailablePortFinder;

import com.google.common.io.Files;
import com.netflix.astyanax.test.EmbeddedCassandra;


/**
 * @TODO - I wanted this in the test module but unfortunately that will create a circular dep
 *         due to the inclusion of the MigrationManager
 */
public class CassandraService {
    private static final Logger LOG = LoggerFactory.getLogger( CassandraService.class );

    public static final CassandraService INSTANCE = new CassandraService();

    private final Object mutex = new Object();

    private EmbeddedCassandra cass;

    private boolean started = false;


    private CassandraService() {

    }


    /**
     * Start the cassandra service
     */
    public void start() throws Throwable {

        if ( started ) {
            return;
        }

        synchronized ( mutex ) {

            //second into mutex
            if ( started ) {
                return;
            }

            File dataDir = Files.createTempDir();
            dataDir.deleteOnExit();

            //cleanup before we run, shouldn't be necessary, but had the directory exist during JVM kill
            if ( dataDir.exists() ) {
                FileUtils.deleteRecursive( dataDir );
            }

            try {
                LOG.info( "Starting cassandra" );

                cass = new EmbeddedCassandra( dataDir, "Usergrid", 9160, AvailablePortFinder.getNextAvailable() );
                cass.start();

                LOG.info( "Cassandra boostrapped" );

                started = true;

                final Socket socket = new Socket();
                final InetSocketAddress address = new InetSocketAddress( "localhost", 9160 );

                while(true) {

                    try {
                        socket.connect( address, 100 );
                    }
                    catch ( ConnectException ce ) {
                        LOG.debug( "Waiting for cassandra to start" );
                        Thread.sleep( 100 );
                    }
                    //we've connected and been disconnected.  Cass is running
                    catch(SocketException se){
                        break;
                    }
                }


                socket.close();
            }
            catch ( IOException e ) {
                throw new RuntimeException( "Unable to start cassandra", e );
            }
        }
    }


    /**
     * Stop the cassandra service
     */
    protected void stop() {
        synchronized ( mutex ) {
            if ( cass != null ) {
                cass.start();
            }
        }
    }
}
