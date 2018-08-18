package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/22/18.
 */

public class SplInsertTask extends AsyncTask<String, Void, Void> {

    @Override
    protected Void doInBackground(String... strings) {
        Socket insertSocket = null;
        String[] ports = {strings[2], strings[3]};
        //String port = strings[2];
        String key = strings[0];
        String value = strings[1];
        //String[] key_value = strings[0].key_values;
        String str = "i"+"######"+key+"#####"+value+"#####"+"SPL";
        Log.v("MYTAG", "string sent on async task(SplInsertTask) for insert :"+str+" to port numbers :"+ports[0]+" "+ports[1]);
        for(int i =0;i<ports.length;i++) {
            try {

                //write the string to the remote port that is supposed to insert the datum
                insertSocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[i]) * 2);
                ObjectOutputStream objectOutputStream = null;
                objectOutputStream = new ObjectOutputStream(insertSocket.getOutputStream());
                objectOutputStream.writeUTF(str);
                objectOutputStream.flush();

                //ObjectInputStream objectInputStream = null;
                ObjectInputStream objectInputStream = new ObjectInputStream(insertSocket.getInputStream());
                String exec_info = objectInputStream.readUTF();
                if (exec_info.compareTo("success") == 0) {
                    Log.v("MYTAG", "replica async task(SplInsertTask) successful");
                }

            } catch (IOException e) {
                Log.v("MYERROR", "SplInsertTask() error");
                e.toString();
            }
        }
        return null;
    }
}