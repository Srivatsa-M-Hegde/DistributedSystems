package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by srivatsa on 4/9/18.
 */

public class msgObject implements Comparable<msgObject> {
    String myport;
    String genhash;

    public msgObject(){
        this.myport = "HELLO";
        this.genhash = "hello";
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
