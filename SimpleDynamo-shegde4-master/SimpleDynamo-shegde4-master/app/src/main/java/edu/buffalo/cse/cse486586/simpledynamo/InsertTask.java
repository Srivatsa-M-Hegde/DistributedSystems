package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/19/18.
 */

public class InsertTask extends AsyncTask<String, Void, Boolean>{

    @Override
    protected Boolean doInBackground(String... strings) {
        Socket insertSocket = null;
        String[] ports = {strings[2], strings[3], strings[4]};
        //String port = strings[2];
        String key = strings[0];
        Boolean res_flag = true;
        String value = strings[1];
        //String[] key_value = strings[0].key_values;
        String str = "i"+"######"+key+"#####"+value+"#####"+"c";
        Log.v("MYTAG", "string sent on co-ordinator async task(InsertTask) for insert :"+str+" to port number :"+ports[0]);

        try {
            //for(int i =0;i<ports.length;i++){
                //write the string to the remote port that is supposed to insert the datum
                insertSocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[0])*2);
                ObjectOutputStream objectOutputStream = null;
                objectOutputStream = new ObjectOutputStream(insertSocket.getOutputStream());
                objectOutputStream.writeUTF(str);
                objectOutputStream.flush();

                //ObjectInputStream objectInputStream = null;
                ObjectInputStream objectInputStream = new ObjectInputStream(insertSocket.getInputStream());
                String exec_info = objectInputStream.readUTF();
                if(exec_info.compareTo("success")==0){
                    Log.v("MYTAG", "co-ordinator async task(InsertTask) successful");
                }
            //}

        } catch (IOException e) {
            Log.v("MYERROR", "InsertTask() error");
            res_flag = false;
            return res_flag;
        }
        return res_flag;
    }
}
