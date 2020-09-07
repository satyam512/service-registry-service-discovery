package clusterManagement;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    /* A ZooKeeper client(zooKeeper) will get various events from the ZooKeeper server it connects to.
     An application using such a client handles these events by registering a callback object with the client. (line 24)
     The callback object is expected to be an instance of a class that implements Watcher interface.
     */
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;

    private final ZooKeeper zooKeeper; // zookeeper client API , this client talks to configured zookeeper server
    private OnElectionEventCallback onElectionEventCallack;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionEventCallback onElectionEventCallack) {
        this.zooKeeper = zooKeeper;
        this.onElectionEventCallack = onElectionEventCallack;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte [] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        /*
            Ephemeral node if gets disconnected and timout occurs, it would get deleted; sequential is telling number
            to be appended at the name is generated sequentially by parent
         */
        System.out.println("Full path znodeName = " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/" , "");
    }

    public void reElection() throws KeeperException, InterruptedException {
        String predecessorZnodeName = null;
        Stat predecessorStat = null;

        while(predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallestChild = children.get(0);
            if(smallestChild.equals(this.currentZnodeName)) {
                System.out.println("I'm the leader ");
                this.onElectionEventCallack.onElectedLeaderEvent();
                return;
            }
            System.out.println("I'm not the leader, " + smallestChild + " is the leader");
            int predecessorIndex = Collections.binarySearch(children, this.currentZnodeName) - 1;
            predecessorZnodeName = children.get(predecessorIndex);
            predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE+"/"+predecessorZnodeName, this);
        }
        this.onElectionEventCallack.onWorkerEvent(); // reaching here means it's definitely a worker now.
        System.out.println("Watching Znode " + predecessorZnodeName);
    }


    public void process(WatchedEvent watchedEvent) {
        //System.out.println(watchedEvent.getType());
        switch (watchedEvent.getType()) {
            case NodeDeleted:
                try {
                    this.reElection();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }

}
