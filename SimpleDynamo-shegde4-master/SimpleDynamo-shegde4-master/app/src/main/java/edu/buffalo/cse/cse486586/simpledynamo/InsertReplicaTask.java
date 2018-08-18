package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.net.UnknownHostException;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/24/18.
 */

public class InsertReplicaTask extends AsyncTask<String, Void, Void> {
    @Override
    protected Void doInBackground(String... strings) {
        Socket insertSocket = null;
        String[] ports = {strings[2], strings[3], strings[4]};
        String temp = "";
        //String port = strings[2];
        String key = strings[0];
        String value = strings[1];
        //String[] key_value = strings[0].key_values;
        String str = "i" + "######" + key + "#####" + value + "#####" + "r";
        Log.v("MYTAG", "string sent on async task(InsertReplicaTask) for insert :" + str + " to port numbers :" + ports[1] + " " + ports[2]);
        for (int i = 1; i < ports.length; i++) {
            try {

                temp = ports[i];
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
                    Log.v("MYTAG", "replica async task(ReplicaInsertTask) successful");
                }


            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (StreamCorruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
