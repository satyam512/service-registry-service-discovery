package clusterManagement;
import java.lang.String;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnodeName;

    private List<String> listOfAddresses;
    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null)
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void registerToCluster(String metadata) throws KeeperException, InterruptedException { // metadata is the data this node wants to share with service registry

        if(this.currentZnodeName!=null) {
            System.out.println("Already registered to service registry");
            return;
        }
        this.currentZnodeName = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to Service Registry");
    }

    public void registerForUpdates() {
        try {
            updateListOfAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public synchronized void updateListOfAddresses() throws KeeperException, InterruptedException { //....(from process() ) but see here only the watcher is being registered so we need to initially explicitly call this method
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        for (String workerZnode : workerZnodes) {

            String fullZnodePath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(fullZnodePath, false);
            if (stat==null)
                continue;       // if while making this list this cluster znode dies
            byte [] addressBytes = zooKeeper.getData(fullZnodePath, false, stat); // see line 33, create method of registerToCluster
            String address = new String(addressBytes);
            addresses.add(address);
        }
        this.listOfAddresses = Collections.unmodifiableList(addresses);
        System.out.println("List of all connected cluster nodes : " + addresses);
    }

    public synchronized List<String> getListOfAddresses() throws KeeperException, InterruptedException {
        if(listOfAddresses==null)
            updateListOfAddresses();
        return listOfAddresses;
    }

    // also we need to de-register in event of this cluster node becoming a leader as now it must avoid communicating with itself
    public void unRegisterFromCluster() {
        try {
            if(currentZnodeName!=null && zooKeeper.exists(currentZnodeName, false)!=null)
                zooKeeper.delete(currentZnodeName, -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case NodeChildrenChanged:
                try {
                    updateListOfAddresses();  // see this will get Triggered when there is any event related to children changes .....
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}

