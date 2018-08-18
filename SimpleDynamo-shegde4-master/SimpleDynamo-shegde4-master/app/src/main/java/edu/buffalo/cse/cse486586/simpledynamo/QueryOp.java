package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by srivatsa on 4/20/18.
 */

public class QueryOp {
    String selection;
    Context context;
    MatrixCursor mc;
    public QueryOp(String selection, Context context, MatrixCursor mc){
        this.selection = selection;
        this.context = context;
        this.mc = mc;
    }
    public MatrixCursor localDataQuery(){
        Log.v("MYTAG", "control in localDataQuery()");
        //if (selection.compareTo("@") == 0) {
            //mc = new MatrixCursor(new String[]{"key", "value"});
            String[] file_list = context.fileList();
            for (int i = 0; i < file_list.length; i++) {
                String[] temp = mc.getColumnNames();
                FileInputStream fs = null;
                try {
                    fs = context.openFileInput(file_list[i]);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                int ch = 0;
                StringBuilder sb = new StringBuilder();
                try {
                    while ((ch = fs.read()) != -1) {
                        char c = (char) ch;
                        sb.append(c);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //Log.v("hello there", "testing this msg");
                mc.addRow(new String[]{file_list[i], sb.toString()});
                //Log.v("MYTAG", " file_list[i] :"+file_list[i]+" sb.toString() :"+sb.toString());
            }
        //}
        return mc;
    }

    public MatrixCursor singleDataQuery(ArrayList<msgObject> arrayList, String myPort){
        try {
            //

            Log.v("MYTAG", "control in SingleDataQuery() function");
            FindKeyPosition fkp = new FindKeyPosition();
            //msgObject[] mob = fkp.find_position(arrayList, selection);
            msgObject[] mob = fkp.find_position(arrayList, selection);
            Log.v("MYTAG", "mob[0].myport :"+mob[0].myport+" mob[1].myport :"+mob[1].myport+" mob[2].myport :"+mob[2].myport);
            //if(genHash(myPort).compareTo(mob[0].genhash)==0) {
            if(genHash(myPort).compareTo(mob[0].genhash)==0) {
                Log.v("MYTAG", "genHash(myPort).compareTo(mob[0].genhash)==0");
                //send a LastNodeQueryTask to the last node
                try {
                    Log.v("MYTAG", "querying the last node");
                    String str = new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[2].myport,selection).get();
                    Log.v("MYTAG", "str :" + str);
                    String[] key_value = str.split("#####");
                    Log.v("MYTAG", " key_value[0] :" + key_value[0] + " key_value[1] :" + key_value[1]);
                    mc.addRow(new String[]{key_value[0], key_value[1]});
                    if(str!=null){
                        return mc;
                    }

                }catch (Exception e){Log.e("MYERROR", "second node queryTask() error");}


                try {
                    Log.v("MYTAG", "querying the last but one node");
                    String str = new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[1].myport,selection).get();
                    Log.v("MYTAG", "str :" + str);
                    String[] key_value = str.split("#####");
                    Log.v("MYTAG", " key_value[0] :" + key_value[0] + " key_value[1] :" + key_value[1]);
                    mc.addRow(new String[]{key_value[0], key_value[1]});
                    if(str!=null){
                        return mc;
                    }

                }catch (Exception e){Log.e("MYERROR", "first node queryTask() error");}



                //if the key is within my internal storage, query
                FileInputStream fs = context.openFileInput(selection);
                int ch = 0;
                StringBuilder sb = new StringBuilder();
                while ((ch = fs.read()) != -1) {
                    char c = (char) ch;
                    sb.append(c);
                }

                mc.addRow(new String[] {selection, sb.toString()});
                //Log.v("MYTAG", "QUERY KEY: "+selection+" "+"QUERY VALUE: "+sb.toString());
            }

            else{
               /* Log.v("MYTAG", "control in else part of singleDataQuery()");
            //if key is not under me create an async task and send it to the appopriate node
                mc = new MatrixCursor(new String[]{"key", "value"});
                Log.v("MYTAG", "column names :"+mc.getColumnName(0)+" :"+mc.getColumnName(1));
                Log.v("MYTAG", "Control being sent to RCQueryTask() from else part of singleDataQuery()");
            String str = new RCQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[0].myport, selection).get();
                Log.v("MYTAG", "Control returned from RCQueryTask() to singleDataQuery()");

            Log.v("MYTAG", "str :"+str);
             String[] key_value = str.split("#####");
            Log.v("MYTAG", " key_value[0] :"+key_value[0]+" key_value[1] :"+key_value[1]);
            mc.addRow(new String[] {key_value[0], key_value[1]});
            Log.v("MYTAG", "query result sent to the appropriate remote node for value regarding :"+key_value[0]+" for value :"+key_value[1]);
            mc.moveToFirst();
            Log.v("MYTAG", "values in remoteQuery function   mc.getString(0) :"+mc.getString(0)+" mc.getString(1):"+mc.getString(1));
            //Log.v("MYTAG", "testing the return of queryTask str :"+str);*/

               //trying to test only the case where the node that receives the query from the grader is the coordinator logic

                //send a LastNodeQueryTask to the last node
                try {
                    Log.v("MYTAG", "querying the last node");
                    String str = new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[2].myport,selection).get();
                    Log.v("MYTAG", "str :" + str);
                    String[] key_value = str.split("#####");
                    Log.v("MYTAG", " key_value[0] :" + key_value[0] + " key_value[1] :" + key_value[1]);
                    mc.addRow(new String[]{key_value[0], key_value[1]});
                    if(str!=null){
                        return mc;
                    }

                }catch (Exception e){Log.e("MYERROR", "second node queryTask() error");}


                try {
                    Log.v("MYTAG", "querying the last but one node");
                    String str = new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[1].myport,selection).get();
                    Log.v("MYTAG", "str :" + str);
                    String[] key_value = str.split("#####");
                    Log.v("MYTAG", " key_value[0] :" + key_value[0] + " key_value[1] :" + key_value[1]);
                    mc.addRow(new String[]{key_value[0], key_value[1]});
                    if(str!=null){
                        return mc;
                    }

                }catch (Exception e){Log.e("MYERROR", "first node queryTask() error");}

                //coordinator
                try {
                    Log.v("MYTAG", "querying the last but one node");
                    String str = new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mob[0].myport,selection).get();
                    Log.v("MYTAG", "str :" + str);
                    String[] key_value = str.split("#####");
                    Log.v("MYTAG", " key_value[0] :" + key_value[0] + " key_value[1] :" + key_value[1]);
                    mc.addRow(new String[]{key_value[0], key_value[1]});
                    if(str!=null){
                        return mc;
                    }

                }catch (Exception e){Log.e("MYERROR", "first node queryTask() error");}
                //testing code ends here

            }
        } catch (Exception e) {
            Log.e("MYERROR", "singleDataQuery code exception");
        }
        return mc;
    }

    public String singleQueryRemote(ArrayList<msgObject> arrayList){
        String str=null;
        try {
            Log.v("MYTAG", "control in singleQueryRemote operation");

            //if the key is within my internal storage, query
                FileInputStream fs = context.openFileInput(selection);
                Log.v("MYTAG", "selection :"+selection);
                int ch = 0;
                StringBuilder sb = new StringBuilder();
                while ((ch = fs.read()) != -1) {
                    char c = (char) ch;
                    sb.append(c);
                }
                str = selection+"#####"+sb.toString();
               // Log.v("MYTAG", "QUERY KEY: "+selection+" "+"QUERY VALUE: "+sb.toString());
            Log.v("MYTAG", "str :"+str);

        } catch (Exception e) {
            Log.e("MYERROR", "DataQueryRemote code exception");
        }
        return str;
    }

    public MatrixCursor allDataQuery(ArrayList<msgObject> arrayList, String myPort){

        QueryOp qO = new QueryOp(selection, context, mc);
        mc = qO.localDataQuery();
        for(int i =0;i<arrayList.size();i++){
            try {
                if(genHash(myPort).compareTo(arrayList.get(i).genhash)!=0){

                    //create an async task and query the remote for stored data, convert the received string to an array and then to a matrix
                    //and then add it to the matrix cursor object
                    //MatrixCursor temp = new MatrixCursor(new String[] {"key", "value"});
                    String temp = "";
                    temp = new AllQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, arrayList.get(i).myport).get();
                    Log.v("MYTAG", "matrix cursor received from AllQueryTask to QueryOp");
                    if(temp.compareTo("")!=0){
                        mc = populateMC(mc, temp);
                    }

                }
            } catch (NoSuchAlgorithmException e) {
                Log.v("MYTAG", "genHash Exception");
            } catch (InterruptedException e) {
                Log.v("MYTAG", "AllQueryTask() exception");
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        Log.v("MYTAG", "returning the mc to the calling function in the query function in SimpleDynamoProvider");
        return mc;
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

    public MatrixCursor populateMC(MatrixCursor mc, String temp){

        Log.v("MYTAG", "control in populateMC() function - QueryOp : temp  :"+temp);
        String[] pairs = temp.split("###");
        for(int i =0;i<pairs.length;i++){
            Log.v("MYTAG", "pairs :"+pairs[i]);
            String[] key_value = pairs[i].split("##");
            Log.v("MYTAG", " key_value[0] :"+key_value[0]+" key_value[1] :"+key_value[1]);
            mc.addRow(new String[] {key_value[0], key_value[1]});
        }

    return mc;
    }


    public String recoverQuery(){
        String str = "";
        Log.v("MYTAG", "control in recoverQuery()");
        //if (selection.compareTo("@") == 0) {
        //mc = new MatrixCursor(new String[]{"key", "value"});
        String[] file_list = context.fileList();
        for (int i = 0; i < file_list.length; i++) {
            String[] temp = mc.getColumnNames();
            FileInputStream fs = null;
            try {
                fs = context.openFileInput(file_list[i]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            int ch = 0;
            StringBuilder sb = new StringBuilder();
            try {
                while ((ch = fs.read()) != -1) {
                    char c = (char) ch;
                    sb.append(c);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Log.v("hello there", "testing this msg");
            //mc.addRow(new String[]{file_list[i], sb.toString()});
            //Log.v("MYTAG", " file_list[i] :"+file_list[i]+" sb.toString() :"+sb.toString());
            str = str+file_list[i]+"##"+sb.toString()+"###";

        }
        //}
        Log.v("MYTAG", "str :"+str);
        return str;
    }

}
