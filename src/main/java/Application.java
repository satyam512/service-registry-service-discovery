import clusterManagement.LeaderElection;
import clusterManagement.OnElectionEventAction;
import clusterManagement.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher{

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper; // zookeeper client API , this client talks to configured zookeeper server

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        Application application = new Application();
        application.connectZooKeeper();
        // need to make the class implementing callback interface
        ServiceRegistry serviceRegistry = new ServiceRegistry(application.getZooKeeper());
        OnElectionEventAction onElectionEventAction = new OnElectionEventAction(currentServerPort, serviceRegistry);


        LeaderElection leaderElection = new LeaderElection(application.getZooKeeper(), onElectionEventAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reElection();

        application.run();
        application.close();

        System.out.println("Disconnected from ZooKeeper server, exiting application");
    }
    public void connectZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();  // timeout in case server is down // that dude was talking about something like hard timout thingy
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Synchronized and connected to Zookeeper server");
                } else { // if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    synchronized (zooKeeper) { // some other thread might be using zookeeper client API somewhere
                        zooKeeper.notifyAll();  // on disconnection it will wake up all threads sleeping on zookeeper client object
                    }
                }
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
}
