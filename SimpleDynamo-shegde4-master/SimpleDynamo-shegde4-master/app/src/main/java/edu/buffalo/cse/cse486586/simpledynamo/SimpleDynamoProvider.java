package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    String portStr;
    String myPort;

    ArrayList<msgObject> arrayList = new ArrayList<msgObject>();
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        //String filename = selection;
        //getContext().deleteFile(selection);
        Log.v("MYTAG", "------------------------grader requested for a delete---------------------------");
        try {
            Log.v("TAG", "deletion for key :"+selection+" requested");
            FindKeyPosition fkp = new FindKeyPosition();
            msgObject[] mob = fkp.find_position(arrayList, selection);
            if(genHash(portStr).compareTo(mob[0].genhash)==0){
                //delete from local storage
                String filename = selection;
                getContext().deleteFile(selection);
                String str = "d"+"######"+selection;
                Log.v("MYTAG", "deleted the key on node that received request(coordinator = initial requested node)");
                new SplDeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, mob[1].myport, mob[2].myport);
            }
            else{
                //create an async task and send it to the appropriate node
                String str = "d"+"######"+selection;
                new DeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, mob[0].myport, mob[1].myport, mob[2].myport);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.v("MYTAG", "--------------------grader requested for an insert--------------------");
        FindKeyPosition fkp = new FindKeyPosition();
        Boolean res = true;
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        try {
            Log.v("MYTAG", "genhash(key) :"+genHash(key));
            msgObject[] mo = fkp.find_position(arrayList, key);
            Log.v("MYTAG", "mo.myport +"+mo[0].myport+" mo.genhash :"+mo[0].genhash);

            //if the key is within my domain
            if(genHash(portStr).compareTo(mo[0].genhash)==0){
                Log.v("MYTAG", "genHash(portStr).compareTo(mo[0].genhash)==0");
                //Log.v("MYTAG", "genHash(portStr).compareTo(mo.genhash)==0");
                String filename = key;
                FileOutputStream fos = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                fos.write(values.get("value").toString().getBytes());
                fos.close();
                Log.v("MYTAG", "inserted key: "+values.get("key").toString()+" "+"inserted value: "+values.get("value").toString());

                //send the values to the replicas
                new SplInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,key,value,mo[1].myport, mo[2].myport);
            }

            //if the key belongs to another node call the async task - insertTask
            else{
                Log.v("MYTAG", "genHash(portStr).compareTo(mo[0].genhash)!=0");
                Log.v("MYTAG", "calling the InsertTask()");
                try{
                    res = new InsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,key,value, mo[0].myport, mo[1].myport, mo[2].myport).get();
                    Log.v("MYTAG", " res :"+res);
                } catch(Exception e){Log.v("MYERROR", "InsertTask() did not return, possibly coordinator failed");
                    res = false;};
                if(res==false){
                    new InsertReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, value, mo[0].myport, mo[1].myport, mo[2].myport);
                }
            }

        } catch (NoSuchAlgorithmException e) {
            Log.v("MYERROR", "find_position error");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("MYTAG", "I am node :"+portStr);

        //hardcoding the ring values
        arrayList = arrayListInitialize(arrayList);
        Log.v("MYTAG", "arrayList.get(0) :"+arrayList.get(0).myport+" arrayList.get(1) :"+arrayList.get(1).myport+" arrayList.get(2) :"+arrayList.get(2).myport+" arrayList.get(3) :"+arrayList.get(3).myport+" arrayList.get(4) :"+arrayList.get(4).myport);

        //call the serverTask and form a infinite while loop
        //ServerSocket serverSocket = null;
        try {
            ServerSocket  serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            //call the client socket to get information from the nodes n-2, n-1 and n+1 and store it in the local storage


        } catch (IOException e) {
            Log.v("MYTAG", "serverSocket creation exception");
        }
        String[] file_list = getContext().fileList();
        for(int i =0;i<file_list.length;i++){
            getContext().deleteFile(file_list[i]);
        }
        //find the nodes that are to be queried for the information
        NodeJoin nj = new NodeJoin(arrayList, portStr, getContext());
        msgObject[] mo = new msgObject[3];
        try {
            mo = nj.find_data_nodes();
            Log.v("MYTAG", "--------------------------------------------------------------------------------------");
            Log.v("MYTAG", "mo[0].myport :"+mo[0].myport+"mo[1].myport :"+mo[1].myport+"mo[2].myport :"+mo[2].myport);
            String str_remote = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mo[0].myport, mo[1].myport, mo[2].myport).get();
            if(str_remote.compareTo("")!=0) {
                Log.v("MYTAG", "str_remote :" + str_remote);
                //nj.store_data(str_remote);
                String[] key_values = str_remote.split("###");
                for (int i = 0; i < key_values.length; i++) {
                    String[] keyValue = key_values[i].split("##");


                /*ContentValues values = new ContentValues();
                values.put("key", keyValue[0]);
                values.put("value", keyValue[1]);*/
                    Log.v("MYTAG", "------------------------------------------");
                    String filename = keyValue[0];
                    Log.v("MYTAG", "key :"+keyValue[0]);
                    Log.v("MYTAG", "value :"+keyValue[1]);
                    //if(keyValue[0]==null){
                    //    Log.v("MYTAG", "keyValue[0](filename) is empty and cannot be stored");
                    //}
                    FileOutputStream fos = null;
                    try {

                        if(is_insertable(filename)==true){
                            Log.v("MYTAG", " filename :"+filename+ " is insertable at me");
                            fos = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            fos.write(keyValue[1].getBytes());
                            fos.close();
                        }


                    } catch (FileNotFoundException e) {
                        Log.v("MYERROR", "could not open the files to store data");
                    } catch (IOException e) {
                        Log.v("MYERROR", "could not write the values to the internal storage");
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            while (true) {
                try {
                    Log.v("MYTAG", "server task waiting to accept...");
                    Socket socket = serverSocket.accept();
                    Log.v("MYTAG", "server task accepts...");
                    //read from the inputstream and check for the type of message received - insert, query or delete
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    String incoming_value = objectInputStream.readUTF();
                    Log.v("MYTAG", "incoming_value :"+incoming_value);

                    //split on ###### to check the message type
                    String[] typeAndMsg = incoming_value.split("######");
                    Log.v("MYTAG", "typeAndMsg :"+typeAndMsg[0]+" "+typeAndMsg[1]);

                    if(typeAndMsg[0].compareTo("i")==0){
                        Log.v("MYTAG", "it is a insert message !");

                        String[] msg_subtype = typeAndMsg[1].split("#####");
                        Log.v("MYTAG", "msg_subtype[0] :"+msg_subtype[0]+"msg_subtype[1] :"+msg_subtype[1]+"msg_subtype[2] :"+msg_subtype[2]);
                        if(msg_subtype[2].compareTo("c")==0){
                            Log.v("MYTAG", "coordinator successfully received the insert message");
                            InsertOp iop = new InsertOp(typeAndMsg[1], getContext());
                            Log.v("MYTAG", "typeAndMsg[1] :"+typeAndMsg[1]);
                            //add to self
                            iop.InsertFunc(arrayList, msg_subtype);
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeUTF("success");
                            objectOutputStream.flush();
                            //objectOutputStream.close();



                        }

                        else{
                            Log.v("MYTAG", "key-value received on the replica from the co-ordinator for insert()");
                            InsertOp iop = new InsertOp(typeAndMsg[1], getContext());
                            Log.v("MYTAG", "typeAndMsg[1] :"+typeAndMsg[1]);

                            iop.InsertReplicaFunc();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeUTF("success");
                            objectOutputStream.flush();
                        }


                    }

                    else if(typeAndMsg[0].compareTo("q")==0){
                        Log.v("MYTAG", "control in query method of serverTask");
                        String incoming_str = typeAndMsg[1];
                        String[] selection_type = incoming_str.split("#####");
                        String selection = selection_type[0];
                        String type = selection_type[1];
                        if(type.compareTo("queryTask")==0){
                            MatrixCursor mc;
                            ObjectOutputStream objectOutputStream = null;
                            mc = new MatrixCursor(new String[]{"key", "value"});
                            QueryOp qOp = new QueryOp(selection, getContext(), mc);
                            String return_remote = "";
                            return_remote = qOp.singleQueryRemote(arrayList);
                            Log.v("MYTAG", "return_remote :"+return_remote);
                            if(return_remote.compareTo("")!=0){
                                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                objectOutputStream.writeUTF(return_remote);
                                objectOutputStream.flush();
                            }

                        }
                        else{
                            Log.v("MYTAG", "control from RCQueryTask() to else part of 'q' of ServerTask");
                            ObjectOutputStream objectOutputStream = null;
                            MatrixCursor mc =  new MatrixCursor(new String[] {"key", "value"});
                            QueryOp qOp = new QueryOp(selection, getContext(), mc);
                            Log.v("MYTAG", "Control sent to singleDataQuery()");
                            mc = qOp.singleDataQuery(arrayList, portStr);
                            Log.v("MYTAG", "Control received from singleDataQuery() to ServerTask");
                            mc.moveToFirst();
                            String key = mc.getString(0);
                            String value = mc.getString(1);
                            String return_remote = key+"#####"+value;
                            Log.v("MYTAG", "return_remote :"+return_remote);
                            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeUTF(return_remote);
                            objectOutputStream.flush();
                            Log.v("MYTAG", "control sent to RCQueryTask from ServerTask");

                        }

                    }

                    else if (typeAndMsg[0].compareTo("a")==0){
                        Log.v("MYTAG", "control in serverTask of remote in the 'a' section");
                        String selection = "DUMMY";
                        MatrixCursor mc = new MatrixCursor(new String[] {"key", "value"});
                        Log.v("MYTAG", "entering the localDataQuery() function of QueryOp");
                        QueryOp qOp = new QueryOp(selection, getContext(), mc);
                        mc = qOp.localDataQuery();
                        Log.v("MYTAG", "control returned from the localDataQuery() function of QueryOp");
                        //stringify the matrixcursor and return

                        String str = StringifyFilelists();
                        Log.v("MYTAG", "(stringified remote data stored locally) str :"+str);

                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(str);
                        Log.v("MYTAG", "writing the local storage that is stringified to the calling AsyncTask - AllQueryTask");
                        objectOutputStream.flush();

                    }
                    else if (typeAndMsg[0].compareTo("d")==0){
                        String selection = typeAndMsg[1];
                        Log.v("MYTAG", "deletion request received for key :"+selection);
                        getContext().deleteFile(selection);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF("success");
                        objectOutputStream.flush();
                        Log.v("MYTAG", "deletion successful sending the success msg back");
                    }

                    else if(typeAndMsg[0].compareTo("rejoin")==0){
                        String return_remote = "";
                        Log.v("MYTAG", "control in rejoin condition of ServerTask() of avd :" +portStr);
                        String selection = "REJOIN";
                        MatrixCursor mc = new MatrixCursor(new String[] {"key", "value"});
                        Log.v("MYTAG", "entering the localDataQuery() function of QueryOp");
                        QueryOp qOp = new QueryOp(selection, getContext(), mc);
                        return_remote = qOp.recoverQuery();
                        Log.v("MYTAG", "control returned from the recoverQuery() function of QueryOp");
                        //stringify the matrixcursor and return

                        //String str = StringifyFilelists();
                        Log.v("MYTAG", "(stringified remote data stored locally) return_remote :"+return_remote);

                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(return_remote);
                        Log.v("MYTAG", "writing the local storage that is stringified to the calling AsyncTask - ClientTask");
                        objectOutputStream.flush();

                    }
                } catch (IOException e) {
                    Log.v("MYERROR", "Server socket did not accept");
                    e.printStackTrace();

                }
            }
            //return null;
        }
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor mc;
        mc = new MatrixCursor(new String[]{"key", "value"});
        QueryOp qOp = new QueryOp(selection, getContext(), mc);
        Log.v("MYTAG", "control in query method");

        Log.v("query", selection);

        if(selection.compareTo("@")==0){
            //local avd key-list query
            Log.v("MYTAG", "calling the @ query function");
            mc = qOp.localDataQuery();

        }

        else if(selection.compareTo("*")==0){
            mc = qOp.allDataQuery(arrayList, portStr);

        }

        else{

            Log.v("MYTAG", "individual key query");
            //MatrixCursor mc1 = new MatrixCursor(new String[]{"key", "value"});
            Log.v("MYTAG", "sent the control to singleDataQuery from query function");

            mc = qOp.singleDataQuery(arrayList, portStr);
            Log.v("MYTAG", "control returned to the cursor method from singleDataQuery");
            //Log.v("MYTAG", "mc values mc.getString(0):"+mc.getString(0)+" mc.getString(1) :"+mc.getString(1));
        }
        return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    public ArrayList<msgObject> arrayListInitialize(ArrayList<msgObject> arrayList){

        String[] portList = {"5554","5556","5558","5560","5562"};
        //msgObject msgObject = new msgObject();
        for(int i=0;i<portList.length;i++){
            try {

                arrayList.add(new msgObject(portList[i], genHash(portList[i])));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(arrayList);
        return arrayList;
    }

    public String StringifyFilelists(){
        String str="";

        String[] file_list = getContext().fileList();
        for (int i = 0; i < file_list.length; i++) {
            //String[] temp = mc.getColumnNames();
            FileInputStream fs = null;
            try {
                fs = getContext().openFileInput(file_list[i]);
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
            str = str+file_list[i]+"##"+sb.toString()+"###";
            Log.v("MYTAG", " file_list[i] :"+file_list[i]+" sb.toString() :"+sb.toString());
        }

        Log.v("MYTAG", " str :"+str);
        Log.v("MYTAG", "returning control to serverTask's calling function condition");
        return str;
       /* String[] key_values = str.split("###");
        String return_str="";
        for(int i =0;i<key_values.length-1;i++){
        }*/
    }

    public boolean is_insertable(String key) throws NoSuchAlgorithmException {
        boolean ret_val = false;
        int ar_size = arrayList.size();
        for(int i =0;i<arrayList.size();i++){
            Log.v("MYTAG", "arrayList.get(i) :"+arrayList.get(i));
        }
        for (int i = 0; i < arrayList.size(); i++) {
            if (arrayList.get(i).genhash.compareTo(genHash(portStr)) == 0) {
                //int plusOne = (i + 1) % ar_size;
                int minus_one = (i + ar_size - 1) % ar_size;
                int minus_two= (i + ar_size - 2) % ar_size;
                int minus_three = (i + ar_size - 3) % ar_size;
                String minusOne = arrayList.get(minus_one).genhash;
                String minusTwo = arrayList.get(minus_two).genhash;
                String minusThree = arrayList.get(minus_three).genhash;
                Log.v("MYTAG", "minus_one :"+minus_one);
                Log.v("MYTAG", "minus_two :"+minus_two);
                Log.v("MYTAG", "minus_three :"+minus_three);

                Log.v("MYTAG", "minusOne :"+minusOne);
                Log.v("MYTAG", "minusTwo :"+minusTwo);
                Log.v("MYTAG", "minusThree :"+minusThree);
                boolean c1=false,c2=false,c3=false;
                if(minusThree.compareTo(minusTwo)>0){
                    Log.v("MYTAG", "if part :genHash(Integer.toString(minusThree)).compareTo(genHash(Integer.toString(minusTwo)))>0");
                    c3 = genHash(key).compareTo(minusThree)>0 || genHash(key).compareTo(minusTwo)<0;
                    Log.v("MYTAG", " c3 :"+c3);
                }
                else{
                    c3 = genHash(key).compareTo(minusThree)>0 && genHash(key).compareTo(minusTwo)<0;
                    Log.v("MYTAG", "else part :genHash(Integer.toString(minusThree)).compareTo(genHash(Integer.toString(minusTwo)))>0");
                    Log.v("MYTAG", " c3 :"+c3);
                }
                if(minusTwo.compareTo(minusOne)>0){
                    Log.v("MYTAG", "if part :genHash(Integer.toString(minusTwo)).compareTo(genHash(Integer.toString(minusOne)))>0");
                    c2 = genHash(key).compareTo(minusTwo)>0 || genHash(key).compareTo(minusOne)<0;
                    Log.v("MYTAG", " c2 :"+c2);
                }
                else{
                    c2 = genHash(key).compareTo(minusTwo)>0 && genHash(key).compareTo(minusOne)<0;
                    Log.v("MYTAG", "else part :genHash(Integer.toString(minusTwo)).compareTo(genHash(Integer.toString(minusOne)))>0");
                    Log.v("MYTAG", " c2 :"+c2);
                }
                if(minusOne.compareTo(genHash(portStr))>0){
                    Log.v("MYTAG", "if part :genHash(Integer.toString(minusOne)).compareTo(genHash(portStr))>0");
                    c1 = genHash(key).compareTo(minusOne)>0 || genHash(key).compareTo(genHash(portStr))<0;
                    Log.v("MYTAG", " c1 :"+c1);
                }
                else{
                    Log.v("MYTAG", "else part :genHash(Integer.toString(minusOne)).compareTo(genHash(portStr))>0");
                    c1 = genHash(key).compareTo(minusOne)>0 && genHash(key).compareTo(genHash(portStr))<0;
                    Log.v("MYTAG", " c1 :"+c1);
                }



                if(c1==true || c2 == true || c3 == true){
                    ret_val = true;
                }
                Log.v("MYTAG", "ret_val :"+ret_val);
                return ret_val;


            }
        }
        return true;
    }

}