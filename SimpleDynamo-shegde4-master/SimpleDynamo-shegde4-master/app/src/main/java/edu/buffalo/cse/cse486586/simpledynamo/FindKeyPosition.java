package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

/**
 * Created by srivatsa on 4/19/18.
 */

public class FindKeyPosition {

    public msgObject[] find_position(ArrayList<msgObject> arrayList, String key) throws NoSuchAlgorithmException {
        msgObject msgObject = new msgObject();
        msgObject first_node =arrayList.get(0);
        Log.v("MYTAG", "ARRAY_LIST_VALUES :"+arrayList.get(0).myport+" "+arrayList.get(1).myport+" "+arrayList.get(2).myport+" "
                +arrayList.get(3).myport+" "+arrayList.get(4).myport);
        //msgObject return_value = new msgObject("hello", genHash("hello"));
        msgObject return_value = null;
        msgObject last_node =arrayList.get(arrayList.size()-1);
        Log.v("MYTAG", "arrayList.get(arrayList.size()-1) :"+arrayList.get(arrayList.size()-1).genhash);
        Log.v("MYTAG", "arrayList.get(0) :"+arrayList.get(0).genhash);
        Log.v("MYTAG", "key :"+genHash(key));
        //if (key< the smallest node)
        if(genHash(key).compareTo(first_node.genhash)<0){
            Log.v("MYTAG", "control in if(genHash(key).compareTo(first_node.genhash)<0) :"+first_node.genhash);
            return_value =  first_node;
        }


        //else if (key > the greatest node)
        else if(genHash(key).compareTo(last_node.genhash)>0){
            Log.v("MYTAG", "control in else if(genHash(key).compareTo(last_node.genhash)>0) :"+last_node.genhash);
            return_value =  first_node;
        }

        //else
        else{
            Log.v("MYTAG", "control in else (findKeyPosition)");
            for(int i=1;i<arrayList.size();i++){
                if(genHash(key).compareTo(arrayList.get(i-1).genhash)>0 && genHash(key).compareTo(arrayList.get(i).genhash)<0){
                    return_value = arrayList.get(i);
                    Log.v("MYTAG", "return_value :"+return_value.myport+" :"+return_value.genhash);
                }
            }
        }
        msgObject[] mob = find_replicas(arrayList, return_value);
        Log.v("MYTAG", "got the replica nodes for :"+mob[0].myport+" they are :"+mob[1].myport+", "+mob[2].myport);
        Log.v("MYTAG", "returning the node and replica msgObject mob to the caller");
        return mob;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public msgObject[] find_replicas(ArrayList<msgObject> arrayList, msgObject return_value){
        msgObject[] mob = new msgObject[3];
        int size = arrayList.size();
        Log.v("MYTAG", "arrayList size :"+size);
        Log.v("MYTAG", "return_value :"+return_value.myport+" :"+return_value.genhash);
        for(int i =0;i<arrayList.size();i++){
            if(arrayList.get(i).genhash.compareTo(return_value.genhash)==0){
                Log.v("MYTAG", "control in line 72 of findKeyPosition");
                Log.v("MYTAG", "(i+1)%size :"+(i+1)%size);
                Log.v("MYTAG", "(i+2)%size :"+(i+2)%size);
                mob[0] = arrayList.get(i);
                Log.v("MYTAG", "mob[0] :"+mob[0].myport);
                mob[1] = arrayList.get((i+1)%size);
                Log.v("MYTAG", "mob[1] :"+mob[1].myport);
                mob[2] = arrayList.get((i+2)%size);
                Log.v("MYTAG", "mob[2] :"+mob[2].myport);
                break;
            }
        }
        Log.v("MYTAG", "mob.length :"+mob.length+" mob[0].port :"+mob[0].myport+" mob[1].port :"+mob[1].myport+" mob[2].port :"+mob[2].myport);
        return mob;
    }
}
