package neu.mapreduce.io.sockets;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * Created by srikar on 4/19/15.
 * This is used by master schedular to keep track of slaves that have been
 * used as mappers and reducers
 */
public class Job {


    private Long jobID;
    private ArrayList<String> mapperSlaveID;
    private ArrayList<String> reducerSlaveID;

    public Job() {
        jobID = System.currentTimeMillis();
        this.mapperSlaveID = new ArrayList<>();
        this.reducerSlaveID = new ArrayList<>();
    }

    public Long getJobID() {
        return jobID;
    }

    public ArrayList<String> getReducerSlaveID() {
        return reducerSlaveID;
    }

    public void setReducerSlaveID(ArrayList<String> reducerSlaveID) {
        this.reducerSlaveID = reducerSlaveID;
    }

    public ArrayList<String> getMapperSlaveID() {
        return mapperSlaveID;
    }

    public void setMapperSlaveID(ArrayList<String> mapperSlaveID) {
        this.mapperSlaveID = mapperSlaveID;
    }

}
