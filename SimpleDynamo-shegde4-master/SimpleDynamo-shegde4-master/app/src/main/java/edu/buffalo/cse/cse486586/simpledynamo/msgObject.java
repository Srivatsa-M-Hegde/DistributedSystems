package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by srivatsa on 4/19/18.
 */

public class msgObject implements Comparable<msgObject> {
    String myport;
    String genhash;

    public msgObject(){
        this.myport = null;
        this.genhash = null;
    }
    public msgObject(String myPort, String genhash){
        this.myport = myPort;
        this.genhash = genhash;

    }

    public int compareTo(msgObject o) {
        // TODO Auto-generated method stub
        if(this.genhash.compareTo(o.genhash)<1){
            return -1;
        }
        else{
            return 1;
        }
    }
}