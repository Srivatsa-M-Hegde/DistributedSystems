package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by srivatsa on 4/19/18.
 */

public class msgRemoteInsert {
    String [] key_values;
    String port;
    public msgRemoteInsert(String port, String[] key_values){
        this.port = port;
        this.key_values = key_values;
    }
}
