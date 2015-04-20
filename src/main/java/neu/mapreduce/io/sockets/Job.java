package neu.mapreduce.io.sockets;

import java.util.HashSet;

/**
 * Created by srikar on 4/19/15.
 */
public class Job {


    private Long jobID;
    private HashSet<String> mapperSlaveID;
    private HashSet<String> reducerSlaveID;

    public Job() {
        jobID = System.currentTimeMillis();
        this.mapperSlaveID = new HashSet<>();
        this.reducerSlaveID = new HashSet<>();
    }

    public Long getJobID() {
        return jobID;
    }

    public HashSet<String> getReducerSlaveID() {
        return reducerSlaveID;
    }

    public void setReducerSlaveID(HashSet<String> reducerSlaveID) {
        this.reducerSlaveID = reducerSlaveID;
    }

    public HashSet<String> getMapperSlaveID() {
        return mapperSlaveID;
    }

    public void setMapperSlaveID(HashSet<String> mapperSlaveID) {
        this.mapperSlaveID = mapperSlaveID;
    }

}
