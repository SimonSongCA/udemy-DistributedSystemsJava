import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Zookeeper Client Threading Model & Zookeeper Java API
 */
public class LeaderElection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    // it will consider the client as dead or disconnect when the session times out
    private static final int SESSION_TIMEOUT = 3000;
    // a one-stop shop for all the communication of our zookeeper server
    private ZooKeeper zooKeeper;

    public static void main(String[] arg) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        // watcher: an event handler that monitors events such as a successful connection, a disconnection,
        // and get the zookeeper API notified.
        // a watcher object needs to implement the 'Watcher' interface and the 'Process' method
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    // close all the resources inside of zookeeper
    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    // the process method will be called by the zookeeper lib on a separate thread
    // whenever there is a new event coming from the zookeeper server.
    @Override
    public void process(WatchedEvent event) {
        // find out what type of event it is
        switch (event.getType()) {
            case None:
                // if we have successfully synchronized with the zookeeper server
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                }
                // the case when losing the connection
                // we will wake up the main thread and allow app to close resources and exit
                else {
                    synchronized (zooKeeper) {
                        // log the disconnection event
                        System.out.println("Disconnected from Zookeeper event");
                        // wake up the main thread: .notifyAll() on the zookeeper object
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}

