package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/29/18.
 */

public class ClientTask extends AsyncTask<String, Void, String>{
    @Override
    protected String doInBackground(String... strings) {
        Socket clientSocket = null;
        String[] query_nodes = new String[3];
        query_nodes[0] = strings[0];
        query_nodes[1] = strings[1];
        query_nodes[2] = strings[2];

        String str = "rejoin"+"######"+"DUMMY";
        String remote_str="";
        String final_str="";
        for(int i =0;i<query_nodes.length;i++){
            try {

                clientSocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(query_nodes[i])*2);
                ObjectOutputStream objectOutputStream = null;
                objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                objectOutputStream.writeUTF(str);
                objectOutputStream.flush();
                Log.v("MYTAG", "request sent from ClientTask to the remote avd :"+query_nodes[i]);
                //receive response from the nodes queried
                ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                remote_str = objectInputStream.readUTF();
                Log.v("MYTAG", "response received at ClientTask() from :"+query_nodes[i]);
                Log.v("MYTAG", "remote_str :"+remote_str);
                /*if(remote_str==""){
                    return "";
                }*/
                final_str = final_str + remote_str;
                //store_data(remote_str);
            } catch (IOException e) {
                Log.v("MYERROR", "error in the ClientTask() (async task) class");
            }

        }
        Log.v("MYTAG", "final_str :"+final_str);
        return final_str;
    }


}