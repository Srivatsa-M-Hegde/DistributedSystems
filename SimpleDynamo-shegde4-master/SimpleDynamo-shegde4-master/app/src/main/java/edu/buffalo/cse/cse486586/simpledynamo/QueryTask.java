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
 * Created by srivatsa on 4/21/18.
 */

public class QueryTask extends AsyncTask<String, Void, String>{

    @Override
    protected String doInBackground(String... strings) {
        String port = strings[0];
        String key = strings[1];
        MatrixCursor mc;
        String return_val=null;
        mc = new MatrixCursor(new String[] {"key", "value"});
        Log.v("MYTAG", "port :"+port+" key :"+key);
        String str = "q"+"######"+key+"#####"+"queryTask";
        Log.v("MYTAG", "str :"+str);
        try {
            Socket querySocket = null;
            querySocket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)*2);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
            objectOutputStream.writeUTF(str);
            objectOutputStream.flush();


            //read the answer sent by the remote node
            ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
            String stringFromRemote = objectInputStream.readUTF();
            Log.v("MYTAG", "stringFromRemote :"+stringFromRemote);
            return_val =  stringFromRemote;
           /* String[] key_value = stringFromRemote.split("#####");
            Log.v("MYTAG", " key_value[0] :"+key_value[0]+" key_value[1] :"+key_value[1]);
            mc.addRow(new String[] {key_value[0], key_value[1]});
            Log.v("MYTAG", "query result sent to the appropriate remote node for value regarding :"+key+" for value :"+key_value[1]);*/
        } catch (IOException e) {
            Log.v("MYERROR", "querySocket error");
        }
        Log.v("MYTAG", "sending the value back to the SingleDataQuery :"+return_val);
        return return_val;
    }

   /* @Override
    protected void onPostExecute(MatrixCursor result)
    {
        *//* Called once task is done; from UI thread, so you can access your widgets *//*

        // Process result as you like
        return_mc(result);
    }
    public MatrixCursor return_mc(MatrixCursor mc){
     return mc;
    }*/
}
