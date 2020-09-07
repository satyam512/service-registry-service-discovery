package clusterManagement;

import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionEventAction implements OnElectionEventCallback {

    private int port;
    private ServiceRegistry serviceRegistry;

    public OnElectionEventAction(int port, ServiceRegistry serviceRegistry) {
        this.port = port;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void onElectedLeaderEvent()  {
        serviceRegistry.unRegisterFromCluster();
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorkerEvent() {
        try {
            String currentServerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
            serviceRegistry.registerToCluster(currentServerAddress);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
