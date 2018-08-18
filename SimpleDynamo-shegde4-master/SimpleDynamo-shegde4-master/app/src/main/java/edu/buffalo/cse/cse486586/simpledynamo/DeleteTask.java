package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/24/18.
 */

public class DeleteTask extends AsyncTask<String, Void, Void>{
    @Override
    protected Void doInBackground(String... strings) {
        //String remotePort = strings[1];
        String[] ports = {strings[1],strings[2],strings[3]};
        String msg = strings[0];
        Socket deleteSocket = null;
        for(int i =0;i<ports.length;i++) {
            try {

                deleteSocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[i]) * 2);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(deleteSocket.getOutputStream());
                objectOutputStream.writeUTF(msg);
                Log.v("MYTAG", "wrote the msg for deletion to remote(coordinator+replicas):" + msg);
                objectOutputStream.flush();

                ObjectInputStream objectInputStream = new ObjectInputStream(deleteSocket.getInputStream());
                String exec_info = objectInputStream.readUTF();
                if(exec_info.compareTo("success")==0){
                    Log.v("MYTAG", "deletion at remote successful");
                }

            } catch (IOException e) {
                e.printStackTrace();
                Log.v("MYERROR", "DeleteTask() error");
            }
        }
        return null;
    }
}
