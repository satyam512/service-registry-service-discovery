package clusterManagement;

public interface OnElectionEventCallback {
    void onElectedLeaderEvent();
    void onWorkerEvent();
}
