package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

/**
 * Created by srivatsa on 4/20/18.
 */

public class InsertOp {
    Uri mUri;
    String str;
    Context context;
    InsertOp(String str, Context context){
        this.str = str;
        this.context = context;
    }
    public void InsertFunc(ArrayList arrayList, String[] msg_subtype){
        Log.v("MYTAG", "str :"+str);
        String[] keyValue = str.split("#####");
        /*ContentValues values = new ContentValues();
        values.put("key", keyValue[0]);
        values.put("value", keyValue[1]);*/
        String filename = keyValue[0];
        FileOutputStream fos = null;
        try {
            fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
            fos.write(keyValue[1].getBytes());
            fos.close();


            //send the query to the other 2 nodes(replicas) from this node(coordinator)
            FindKeyPosition fkp = new FindKeyPosition();
            msgObject[] mo = fkp.find_position(arrayList, msg_subtype[0]);
            String key = msg_subtype[0];
            String value = msg_subtype[1];
            new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,key,value, mo[0].myport, mo[1].myport, mo[2].myport);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            Log.v("MYERROR", "Line 42 in InsertFunc() of InsertOp");
        }

        Log.v("MYTAG", "keyValue[0] :"+keyValue[0]+" keyValue[1] :"+keyValue[1]);

        Log.v("MYTAG", "inserted the value received from remote at the co-ordinator");
    }

    public void InsertReplicaFunc(){
        Log.v("MYTAG", "inside the InsertReplicaFunc()");
        Log.v("MYTAG", "str :"+str);
        String[] keyValue = str.split("#####");

        String filename = keyValue[0];
        FileOutputStream fos = null;
        try {
            fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
            fos.write(keyValue[1].getBytes());
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.v("MYTAG", "keyValue[0] :"+keyValue[0]+" keyValue[1] :"+keyValue[1]);

        Log.v("MYTAG", "inserted the value received from remote at the replica");
    }

}
