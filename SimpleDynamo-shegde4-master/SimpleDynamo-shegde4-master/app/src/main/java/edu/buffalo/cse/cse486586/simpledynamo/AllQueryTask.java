package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static java.net.InetAddress.getByAddress;

/**
 * Created by srivatsa on 4/23/18.
 */

public class AllQueryTask  extends AsyncTask<String, Void, String>{
    @Override
    protected String doInBackground(String... strings) {
        String remotePort = strings[0];
        String stringFromRemote = "";
        Log.v("MYTAG", "remotePort :"+remotePort);
        String str = "a"+"######"+"DUMMY";
        Log.v("MYTAG", " str :"+str);

        //send the query to the remote for its local storage data
        Socket querySocket = null;
        try {
            querySocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)*2);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
            objectOutputStream.writeUTF(str);
            objectOutputStream.flush();


        //get the data returned by remote node with its local data in the form of a string, convert to array
            Log.v("MYTAG", "control back in AsyncTask - AllQueryTask");
            ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
            stringFromRemote = objectInputStream.readUTF();
            Log.v("MYTAG", "stringFromRemote(received at AllQueryTask) :"+stringFromRemote);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return stringFromRemote;
    }
}
