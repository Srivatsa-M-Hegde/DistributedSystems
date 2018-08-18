package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.util.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

/**
 * Created by srivatsa on 4/29/18.
 */

public class NodeJoin {
    ArrayList<msgObject> arrayList = new ArrayList();
    String myport;
    Context context;

    NodeJoin(ArrayList arrayList, String myport, Context context) {
        this.arrayList = arrayList;
        this.myport = myport;
    }

    public msgObject[] find_data_nodes() throws NoSuchAlgorithmException {
        msgObject[] mo = new msgObject[3];
        int ar_size = arrayList.size();
        for (int i = 0; i < arrayList.size(); i++) {
            if (arrayList.get(i).genhash.compareTo(genHash(myport)) == 0) {
                int next = (i + 1) % ar_size;
                int prePrev = (i + ar_size - 1) % ar_size;
                int prev = (i + ar_size - 2) % ar_size;
                mo[2] = arrayList.get(next);
                mo[0] = arrayList.get(prePrev);
                mo[1] = arrayList.get(prev);
            }
        }
        return mo;
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


    public void store_data(String str) {
        if(str.compareTo("")!=0){
            String[] key_values = str.split("###");
            for (int i = 0; i < key_values.length; i++) {
                String[] keyValue = key_values[i].split("##");


                /*ContentValues values = new ContentValues();
                values.put("key", keyValue[0]);
                values.put("value", keyValue[1]);*/
                String filename = keyValue[0];
                Log.v("MYTAG", "key :"+keyValue[0]);
                Log.v("MYTAG", "value :"+keyValue[1]);
                //if(keyValue[0]==null){
                //    Log.v("MYTAG", "keyValue[0](filename) is empty and cannot be stored");
                //}
                FileOutputStream fos = null;
                try {
                    Log.v("MYTAG", " filename :"+filename);
                    fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
                    fos.write(keyValue[1].getBytes());
                    fos.close();

                } catch (FileNotFoundException e) {
                    Log.v("MYERROR", "could not open the files in store_data() method");
                } catch (IOException e) {
                    Log.v("MYERROR", "could not write the values to the internal storage");
                }
            }
        }

    }

    public Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}
