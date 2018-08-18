package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider;
import java.net.ServerSocket;
import java.util.ArrayList;

/**
 * Created by srivatsa on 4/16/18.
 */

class ServerTask extends AsyncTask<ServerSocket, String, Void>{
    @Override
    protected Void doInBackground(ServerSocket... serverSockets) {
        //if()


        return null;
    }

    public String NLIGenerator(ArrayList<String> al, String str){
        int j;
        String pre;
        String rtn_str = null;
        for(int i=0;i<al.size();i++){
            j = str.compareTo(al.get(i));
            if (j==0){
                if(al.size()==1){
                    pre = al.get(0);
                }
                else{
                    pre = al.get(i-1);
                }
                String node = al.get(i);
                String succ;
                if(i+1==al.size()){
                    succ = al.get(0);
                }
                else{
                    succ = al.get(i+1);
                }
                rtn_str = pre+"##"+node+"##"+succ;
            }
            break;
        }

        return rtn_str;
    }

}
