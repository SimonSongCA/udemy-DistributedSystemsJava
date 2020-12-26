import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

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
    // define the z-node prefix
    private static final String ELECTION_NAMESPACE = "/election";
    // the currentZNodeName variable used to store the current name of the z-node
    private String currentZnodeName;

    public static void main(String[] arg) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();

        // volunteer as a leader and elect a leader.
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        // znode prefix. 'c' stands for 'candidate'
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        // call the znode create() method to create a znodeFullPath using znodePrefix
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // print out the znode full path
        System.out.println("znode name " + znodeFullPath);
        // the shortened version of the path is used to assign the variable 'currentZnodeName'
        this.currentZnodeName = znodeFullPath.replace("/election/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        // local variables
        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        // need a while loop since the znode that we are interested in watching in the
        // .exist() method may be already gone before we call it.
        // This is the dynamic nature of the cluster.
        while (predecessorStat == null) {
            // call getChildren() to get a list of the children z-node of the election z-node
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            // sort the list
            Collections.sort(children);
            // get the child with the smallest index. This will be the leader among all the other z-nodes
            String smallestChild = children.get(0);
            // determine whether the currentZnodeName is the smallest one.
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                return;
            }
            // more actions to take when the current node is not a leader
            else {
                System.out.println("I am not the leader");
                // find the previous predecessor znode that the current node needs to watch for failures.
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                // get the name of the predecessor name
                predecessorZnodeName = children.get(predecessorIndex);
                // fetch the stats of the predecessor node.
                // a 'watch' object is the something we use to get notified when the predecessor node gets deleted
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
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
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (InterruptedException e) {
                } catch (KeeperException e) {
                }
        }
    }
}

