package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.NotSerializableException;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.graphics.Matrix;
import android.net.Uri;
import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import android.database.MatrixCursor;

import org.w3c.dom.Node;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    private static final int SERVER_PORT = 10000;
    private static final String masterNode = "5554";

    private static final String[] DBCOL = {"key", "value"};

    private static HashMap<String, messgInfo> Localstorage = new HashMap<String, messgInfo>();
    //    private static HashMap<String, messgInfo> OwnerStorage = new HashMap<String, messgInfo>();
//    private static HashMap<String, messgInfo> NeighStorage = new HashMap<String, messgInfo>();
    private static TreeMap<String, String> NodeHashAddress = new TreeMap<String, String>();
    private static String firstNodeHash;
    private static String lastNodeHash;
    public  static boolean wait = false;
    private String selfNodeID, succNodeID, preNodeID;
    private final packet globalPacketStack = new packet();
    private boolean exception = false;
    String neigh1, neigh2, preneigh1, preneigh2;
    @Override



    public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        Log.e("DYNAMO ::"+"DELETING ::"," I HAVE ENTERED FUNCTION");
        packet pack;
        boolean STARTNODE = (uri != null);
        String key = selection;
        String selfHash = findNodeID(selfNodeID);
        if (selection.equals("@") || selection.equals("*"))
        {
            Log.e("DYNAMO ::"+"DELETE @ OR *::"," I HAVE ENTERED");
            Localstorage.clear();
            //OwnerStorage.clear();
            //NeighStorage.clear();

            // --------------- IF BOSS THAN ENTER BELOW TO ASK SLAVES TO DELETE ELSE RETURN --------------- //

            if (selection.equals("*") && succNodeID.equals(selectionArgs[0]) == true)
            {
                pack = new packet(null, selection, null, selectionArgs[0], null, selectionArgs[0], QueryNature.DELETE, 0);
//                new AskForkeys().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                sendTo(pack);
            }
            return 0;
        }

        else if(matchMe(selection))
        {
            Log.e("DYNAMO ::" + "DELETE ::", "DELETING THE KEY ::" + selection);
            Localstorage.remove(selection);
            //OwnerStorage.remove(selection);

            pack = new packet(neigh1, selection, null, selfNodeID, null, selfNodeID, QueryNature.DELETE, 2);

            sendTo(pack);
        }
        else // ------------ CONTACT SLAVE DIRECTLY AS HASH KEY NOT MY HASH ------------- //
        {
            if(Localstorage.containsKey(key))
            {
                Localstorage.remove(key);
            }
            Log.e("DYNAMO ::"+"DELETE NOT MY KEY::","DELETING THE KEY ::"+selection+"");
            String destAdd = matchKey(key);
            pack = new packet(destAdd, selection, null, selfNodeID, null, selfNodeID, QueryNature.DELETE, 0);
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
            sendTo(pack);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        String key = values.getAsString(DBCOL[0]);
        String value = values.getAsString(DBCOL[1]);
        String rightFulOwner = matchKey(key);
        messgInfo messg = new messgInfo(value, rightFulOwner);
        String destAdd = matchKey(key);

        Log.e("DYNAMO ::" + "INSERT ::STORE ::", "LOCAL KEY ::" + key + " " + value + "");
        if(Localstorage.containsKey(key))
        {
            Localstorage.remove(key);
        }
        Localstorage.put(key, messg);
        if (matchMe(key))// || AmFirstKeyGreaterThanAll(key))
        {
            Log.e("DYNAMO ::" + "INSERT ::", "IT'S MY HASH KEY ::" + key + "");
//                packet pack = new packet(neigh1, key, value, null, null, null, QueryNature.INSERT, 2);
//                new AskForkeys().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
        }
        else
        {
            Log.e("DYNAMO ::"+"INSERT ::", "NOT MY HASH KEY ::" + key);
            Log.e("DYNAMO ::"+"INSERT ::", "NOW SENDING KEY ::" + key + "TO ::" + destAdd + "");

//                packet pack = new packet(destAdd, key, value, null, null, null, QueryNature.INSERT, 0);
//                new AskForkeys().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
        }
        packet pack = new packet(destAdd, key, value, null, null, null, QueryNature.INSERT, 0);
        //new AskForkeys().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
        sendTo(pack);
        return uri;
    }

    public void sendTo(packet p)
    {

        boolean localExcept = false;
/*            int count = 0;
            while(count < 2)
            {
                count++;
*/
        Log.e("ASKFORKEYS ::","KEY ::"+p.key);
        for (Map.Entry<String, String> entry : NodeHashAddress.entrySet()) {
            String destAdd = entry.getValue();
            Socket socket;
            Log.e("DYNAMO ::" + "CLIENT_TASK ::", "I AM THE DESTINY ::" + destAdd + " KEY ::" + p.key + " VALUE ::" + p.value);
            Log.e("DYNAMO ::" + "CLIENT_TASK ::DEST ::", destAdd + "");
            Log.e("DYNAMO ::" + "CLIENT_TASK ::NAT ::", "" + p.QueryNature + "");
            Log.e("DYNAMO ::" + "CLIENT_TASK ::KEY ::", p.key + "");
            if (destAdd.equals(selfNodeID)) {
                continue;
            }

            try {

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * (Integer.parseInt(destAdd)));
                socket.setSoTimeout(1000);
                OutputStream s = socket.getOutputStream();
                ObjectOutputStream obj = new ObjectOutputStream(s);
                obj.writeObject(p);
                obj.close();
                socket.close();

//                    break;
            }catch(UnknownHostException e){
                Log.e("DYNAMO ::" + "" + TAG, "CLIENT_TASK :: UN_KNOWN_HOST\n\n");
                localExcept = true;
                Log.e("DYNAMO ::" + "CLIENT_TASK ::", "FAILED TO SEND TO ::" + p.destiny);
            }catch(IOException e){
                localExcept = true;
                e.printStackTrace();
                Log.e("DYNAMO ::" + "" + TAG, "CLIENT_TASK :: SOCKET IO EXCEPTION ::" + e.getCause() + "");
                Log.e("DYNAMO ::" + "CLIENT_TASK ::", "FAILED TO SEND TO ::" + p.destiny);
            }
        }
        //    return null;


    }
    public String matchKey(String key) {
        String keyHash = findNodeID(key);
        String higher = NodeHashAddress.higherKey(keyHash);

        if (NodeHashAddress.containsKey(keyHash))
        {
            Log.e("DYNAMO ::"+"MATCH_KEY 1::", "FOUND RIGHTFUL KING SELF & SELFHASH ::" + NodeHashAddress.get(keyHash) + " " + keyHash);
            return NodeHashAddress.get(keyHash);
        }
        else if (higher != null)
        {
            Log.e("DYNAMO ::"+"MATCH_KEY 2::", "FOUND RIGHTFUL KING SELF & SELFHASH ::" + NodeHashAddress.get(higher) + " " + higher);
            return NodeHashAddress.get(higher);
        }

        Log.e("DYNAMO ::"+"MATCH_KEY 3::", "FOUND RIGHTFUL KING SELF & SELFHASH ::" + NodeHashAddress.firstKey() + " " + NodeHashAddress.firstEntry().getValue());
        return NodeHashAddress.firstEntry().getValue();
    }

    public void StoreHashNodeAddr() {
        NodeHashAddress.put(findNodeID(Integer.toString(5554)), Integer.toString(5554));
        NodeHashAddress.put(findNodeID(Integer.toString(5556)), Integer.toString(5556));
        NodeHashAddress.put(findNodeID(Integer.toString(5558)), Integer.toString(5558));
        NodeHashAddress.put(findNodeID(Integer.toString(5560)), Integer.toString(5560));
        NodeHashAddress.put(findNodeID(Integer.toString(5562)), Integer.toString(5562));


        firstNodeHash = NodeHashAddress.firstKey();
        lastNodeHash = NodeHashAddress.lastKey();

        Log.e("DYNAMO ::"+"STORE HASH ::", "AS FOLLOWS ::");
        String selfHash = findNodeID(selfNodeID);
        preNodeID = NodeHashAddress.lowerKey(selfHash);
        if(preNodeID == null)
        {
            preNodeID = NodeHashAddress.lastKey();
        }

        neigh1 = NodeHashAddress.higherKey(selfHash);
        if(neigh1 == null)
        {
            neigh1 = NodeHashAddress.firstKey();
        }

        neigh2 = NodeHashAddress.higherKey(neigh1);
        if(neigh2 == null)
        {
            neigh2 = NodeHashAddress.firstKey();
        }



        preneigh1 = NodeHashAddress.lowerKey(selfHash);
        if(preneigh1 == null)
        {
            preneigh1 = NodeHashAddress.lastKey();
        }

        preneigh2 = NodeHashAddress.lowerKey(preneigh1);
        if(preneigh2 == null)
        {
            preneigh2 = NodeHashAddress.lastKey();
        }

        preNodeID = NodeHashAddress.get(preNodeID);
        neigh1 = NodeHashAddress.get(neigh1);
        neigh2 = NodeHashAddress.get(neigh2);
        preneigh1 = NodeHashAddress.get(preneigh1);
        preneigh2 = NodeHashAddress.get(preneigh2);
        Log.e("DYNAMO ::"+"STORE HASH ::",preNodeID+" "+preneigh1+" "+preneigh2+" "+selfNodeID+" "+neigh1+" "+neigh2);
    }


//    private

    private String findNodeID(String key)
    {
        try
        {
            return genHash(key);
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.e("DYNAMO ::"+TAG, "GEN_HASH EXCEPTION ::" + e.getMessage());
        }
        return "";
    }


    private boolean matchMe(String key)
    {
        if(matchKey(key).equals(selfNodeID))
        {
            return true;
        }

        return false;
    }

    private String getSuccOfNode(String node)
    {
        String nodeHash = findNodeID(node);
        String succNodeHash = NodeHashAddress.higherKey(nodeHash);
        if(succNodeHash == null)
        {
            succNodeHash = NodeHashAddress.firstKey();
        }
        return NodeHashAddress.get(succNodeHash);
    }

    @Override
    public boolean onCreate()
    {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        selfNodeID = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        Log.e("DYNAMO ::" + "ONCREATE ::SELF ID ::", selfNodeID + "");
        preNodeID = succNodeID = "";
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("DYNAMO ::"+TAG, "ONCREATE :: CANNOT CREATE A SERVER_SOCKET\n\n");
            return false;
        }

        StoreHashNodeAddr();
        AskNeighForKeys();
        return true;
    }

    public void AskNeighForKeys()
    {
        // ================= Take keys from the predecessor and successor neighbour ================ //
        packet pckt = new packet(null, selfNodeID, QueryNature.REQUEST_KEYS);
        new AskForkeys().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pckt);

    }

    // TODO ::
    // 1. IF GENHASH EQUAL TO MY SELFNODEID FETCH KEY VALUE AND RETURN CURSOR
    // 2. ELSE FORWARD TO THE DESTINY NODE SUCCESSOR WITH NATURE QUERY AND SET TLL TO 3
    // 3. DESTINY WILL SUBTRACT TTL BY 1 AND RETURN
    // 4. IF "*" CALLED CALL EACH INDIVIDUALLY AND ASK RETURN MAP
    public Cursor query(packet pckt) {
        Log.e("DYNAMO ::"+"QUERY", " :: " + pckt.key + "");
        MatrixCursor cursor = new MatrixCursor(DBCOL);
        packet pack;
        String key = pckt.key;
        String initNode = pckt.initNode;

        Log.e("DYNAMO ::"+"QUERY ::", "" + key.charAt(0));

        switch (key.charAt(0)) {
            case '@':
                // -------------- I AM THE BOSS I AM THE SLAVE :: NO FORWARD JUST RETURN ---------------- //
                Log.e("DYNAMO ::"+"QUERY :: @ CALLED", "KEY ::" + key);
                for (Map.Entry<String, messgInfo> mpIter : Localstorage.entrySet()) {
                    String prt = mpIter.getValue().port;
                    if (prt.equals(preneigh1) || prt.equals(preneigh2) || prt.equals(selfNodeID)) {

                        Log.e("DYNAMO ::" + "QUERY ::", "INSIDE LOOP KEY ::" + mpIter.getKey() + "VALUE ::" + mpIter.getValue().value + "");
                        cursor.addRow(new String[]{mpIter.getKey(), mpIter.getValue().value});

                    }
                }
                break;
            case '*':
                Log.e("DYNAMO ::"+"QUERY ::","I AM IN *");

                // --------------------- I AM THE BOSS I AM THE ONLY NODE --------------------------- //
                if (preNodeID.length() == 0) {
                    Log.e("DYNAMO ::"+"QUERY *::","I AM ALONE");
                    for (Map.Entry<String, messgInfo> mpIter : Localstorage.entrySet())
                        cursor.addRow(new String[]{mpIter.getKey(), mpIter.getValue().value});
                    break;
                }

                // ---------- FILL THE PACKET DATABASE NO MATTER YOU ARE MASTER OR SLAVE ------------ //
                pckt.DBcursor.putAll(Localstorage);
                Log.e("DYNAMO ::"+"QUERY * ::","GOING TO SLAVE");

                // ------------------------ I AM THE SLAVE ---------- RETURN TO BOSS ---------------- //
                if (!selfNodeID.equals(pckt.initNode)) {
                    Log.e("DYNAMO ::"+"QUERY *::","I AM THE SLAVE");
                    pckt.destiny = pckt.initNode;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pckt);
//					break;
                    //                   replyBack(pckt);
                    break;
                }

                // --------------------------------- I AM THE BOSS ---------------------------------- //
                // --------------- CALL EACH SLAVE TO PUT ALL LOCAL STORAGE AND SEND BACK ----------- //
                // ===================== NO NEED TO HANDLE FAILURE AS LOOPING OVER ALL NODES ====================== //
                Log.e("DYNAMO ::"+"INSERT *::","I AM THE BOSS GOING TO CALL");
                for (Map.Entry<String, String> entry : NodeHashAddress.entrySet())
                {
                    String destAdd = entry.getValue();
                    if (destAdd.equals(selfNodeID))
                    {
                        continue;
                    }
                    Log.e("DYNAMO ::"+"INSERT ::","CALL EACH SLAVE ADD ::"+destAdd);
                    pack = new packet(destAdd, pckt, true);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);

                    synchronizer(globalPacketStack);
                }

                Log.e("DYNAMO ::"+"QUERY ::STORE ::", "* RESULT FROM SYSTEM\n\n");

                //    if (initNode.equals(selfNodeID)){//uri != null) {
                for (Map.Entry<String, messgInfo> mpIter : globalPacketStack.DBcursor.entrySet()) {
                    Log.e("DYNAMO ::"+"QUERY *::", "KEY ::" + key + "VALUE ::" + mpIter.getValue().value + "");
                    cursor.addRow(new String[]{mpIter.getKey(), mpIter.getValue().value});
                }
                //  }
                break;

            // -------------------------------------- QUERY FOR SPECIFIC KEY  --------------------------------------- //
            // 1. IF KEY BELONGS TO ME RETURN CURSOR
            // 2. ELSE DIRECTLY CONTACT THE SLAVE AND WAIT FOR HIS RESPONSE
            default:
                // ================ TODO :: MUST BE MY HASH RETURN VALUE IN THE CURSOR
                String destAdd = matchKey(key);
                Log.e("DYNAMO ::"+"QUERY ::", " FOR SPECIFIC KEY ::" + key + "");
                if(Localstorage.containsKey(key))
                {
                    Log.e("DYNAMO ::QUERY ::","I GOT THE KEY RETURNING KEY ::"+key);
                    cursor.addRow(new String[]{key, Localstorage.get(key).value});
                }
                else if (matchMe(key)) {
                    Log.e("DYNAMO ::"+"QUERY ::DEFAULT ::", "MY HASH MATCHES \n\n");
                    boolean checkempty = (initNode != null && initNode.length() != 0);
                    boolean AM_NOT_BOSS = (checkempty && (!initNode.equals(selfNodeID)));

                    /* DONE---------- I AM SLAVE, RETURN BACK TO BOSS -------------- */
                    if (AM_NOT_BOSS) {
                        Log.e("DYNAMO ::"+"QUERY ::DEFAULT :: ", "RETURN QUERY RESULT TO BOSS\n\n");
                        pack = new packet(initNode, key, Localstorage.get(key).value, pckt);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                        //                       replyBack(pack);
                        break;
                    }

                    // ----------------------- I AM THE BOSS ---------------------- //
                    else {
                        if(!Localstorage.containsKey(key))
                        {
                            wait = true;
                            while(wait);
                        }
                        cursor.addRow(new String[]{key, Localstorage.get(key).value});
                        Log.e("DYNAMO ::"+"QUERY ::DEFAULT ::", "LOCAL STORAGE :: " + Localstorage.get(key) + "");
                    }
                    Log.e("DYNAMO ::"+"QUERY ::", "LOCAL :: VALUE FOUND ::" + Localstorage.get(key) + "");
                    break;
                }
                else {
                    // ------------- I AM THE BOSS, I DONT HAVE THE KEY ----------- //
                    // ------------- SEARCH FOR THE SLAVE
                    // ============= TODO :: FAILURE DETECTION CHANGES :: IF SLAVE DOESN'T RESPOND ASK SUCCESSOR
                    // ============= TODO :: SUCCESSOR WILL CALL RIGHTFUL OWNER AND WILL FIND IT DEAD AND RETURN THE KEY BY SELF
                    if(Localstorage.containsKey(key))
                    {
                        Log.e("QUERY ::","I GOT THE KEY RETURNING KEY ::"+key);
                        cursor.addRow(new String[]{globalPacketStack.key, globalPacketStack.value});
                        break;
                    }
                    Log.e("DYNAMO ::"+"QUERY ::SUCCID ::", succNodeID + " " + key + "");
                    pack = new packet(destAdd, pckt, true);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);

                    // ------------- WAIT FOR MY SLAVE TO RETURN MY PACKET ---------------- //
                    if (initNode != null && initNode.equals(selfNodeID)){//uri != null) {
                        synchronizer(globalPacketStack);
                        cursor.addRow(new String[]{globalPacketStack.key, globalPacketStack.value});
                        Log.e("DYNAMO ::"+"QUERY 11::", "SLAVE RETURNED KEY ::" + globalPacketStack.key + " VALUE ::" + globalPacketStack.value);

                    }
                }
        }
        Log.e("DYNAMO ::"+"QUERY 22::", "SLAVE RETURNED KEY ::" + key + " VALUE ::" + globalPacketStack.value);
        return cursor;
    }

    public void synchronizer(packet pack) {
        wait = true;
        synchronized(pack) {
            try {
                pack.wait();
            } catch (InterruptedException e) {
                Log.e("DYNAMO ::"+TAG, "QUERY INTERRUPT");
            }
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        while(wait);
        packet pckt = new packet(selection, selfNodeID, QueryNature.QUERY);

        Log.e("DYNAMO ::"+"QUERY ::GRADER ::", selection);
        return query(pckt);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream packetStream = new ObjectInputStream(socket.getInputStream());
                    packet pack = (packet) packetStream.readObject();
                    Log.e("DYNAMO ::"+"SERVER ::","YOU ARE NEEDED BOSS KEY ::"+pack.key);
                    whose_pack(pack);
                } catch (UnknownHostException e) {
                    Log.e("DYNAMO ::"+TAG, "EXCEPTION :: SERVER_TASK :: UN_KNOWN_HOST\n\n");
                } catch (IOException e) {
                    Log.e("DYNAMO ::"+TAG, "EXCEPTION :: SERVER_TASK :: SOCKET IO\n" + e.getMessage() + "");
                } catch (ClassNotFoundException e) {
                    Log.e("DYNAMO ::"+TAG, "EXCEPTION :: SERVER_TASK :: CLASS_NOT_FOUND\n\n");
                }
            }

            //return null;
        }

    }

    public void whose_pack(packet pack) {
        Log.e("DYNAMO ::"+"WHOSE_PACK ::","SENDER ::"+pack.initNode);
        switch (pack.QueryNature) {
            case REQUEST_KEYS:
                // ================= DONT SEND BACK AND WASTE TIME IF LOCAL STORAGE IS EMPTY =================== //
                Log.e("DYNAMO ::"+"WHOSE_PACK ::","REQUEST_KEYS ::REQUESTER OF KEYS ::"+pack.initNode);
                if(Localstorage.isEmpty())
                {
                    Log.e("DYNAMO ::"+"WHOSE_PACK ::","NOT SENDING AS LOCAL STORAGE EMPTY");
                    break;
                }
                Log.e("DYNAMO ::"+"WHOSE_PACK ::","REQUEST_KEYS ::LOCAL STORAGE NOT EMPTY SO SENDING TO ::"+pack.initNode);
                pack.destiny = pack.initNode;
                pack.initNode = selfNodeID;
                pack.QueryNature = QueryNature.RECEIVE_KEYS;
                pack.DBcursor.putAll(Localstorage);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                break;
            case RECEIVE_KEYS:
                Log.e("DYNAMO ::"+"WHOSE_PACK ::","RECEIVE_KEYS ::RECEIVING KEYS FROM ::"+pack.initNode);
                Localstorage.putAll(pack.DBcursor);
                wait = false;
                break;
            case INSERT:
                // ---- TODO ::
                // ---- 1. IF KEY GEN_HASH TRUE FOR ME, STORE, SET TTL TO 2 AND FORWARD (WOULD BE DIRECTLY CONTACTED SO SHOULD SAVED)
                // ---- 2. ELSE IF TTL SET, STORE THE PAIR RETURN
                String destAdd = matchKey(pack.key);
                messgInfo messg = new messgInfo(pack.value, destAdd);
                if(Localstorage.containsKey(pack.key))
                {
                    Localstorage.remove(pack.key);
                }
                Localstorage.put(pack.key, messg);
          /*      if (pack.TTL > 0) {
                    Log.e("DYNAMO ::" + "WHOSE_PACK ::", "TTL FOUND GREATER THAN 0 ADD KEY ::" + pack.key);

                    // ******************* IF TTL == 2 ITS JUST PRED ELSE ITS PRED OF PRED ******************* //

//                    NeighStorage.put(pack.key, pack.value);
                    if(pack.TTL == 2)
                    {
                        Log.e("DYNAMO ::"+"SERVER INSERT ::","TTL FOUND EQUAL TO 2 FORWARD ::"+pack.key);
                        pack = new packet(neigh1, pack.key, pack.value, null, null, null, QueryNature.INSERT, 1);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                    }

                    break;
                }
                ContentValues cv = new ContentValues();
                cv.put(DBCOL[0], pack.key);
                cv.put(DBCOL[1], pack.value);
                insert(null, cv);
                break;*/
            case QUERY:
                // TODO :: IF PACKET RESPONDED BACK FROM ANOTHER NODE ASSIGN TO GLOBAL COLLECTION STACK AND RETURN //
                if (selfNodeID.equals(pack.initNode)) {
                    Log.e("DYNAMO ::"+"QUERY :: IN PACK ::", pack.key + " " + pack.value + "");
                    Log.e("DYNAMO ::"+"QUERY ::WHOSE_PACK ::","GOING TO REMOVE THE WAIT");
                    globalPacketStack.key = pack.key;
                    globalPacketStack.value = pack.value;
                    globalPacketStack.DBcursor.putAll(pack.DBcursor);
                    wait = false;

                    synchronized(globalPacketStack) {
                        globalPacketStack.notify();
                    }
                    break;
                }
                Log.e("DYNAMO ::"+"WHOSE_PACK ::","QUERY");
                query(pack);
                break;
            case DELETE:
//                if (pack.TTL > 0) {
                Log.e("DYNAMO ::" + "SERVER DELETE ::", "TTL FOUND GREATER THAN 0 ADD KEY ::" + pack.key);
                if(pack.key.charAt(0) == '*')
                {
                    Localstorage.clear();
                }
                else
                {
                    Localstorage.remove(pack.key);
                }
                //NeighStorage.remove(pack.key);

/*                    if(pack.TTL == 2)
                    {
                        Log.e("DYNAMO ::"+"DELETE ::","TTL FOUND 2 ASKING MY NEIGH TO DELETE KEY ::"+pack.key);
                        pack = new packet(neigh1, pack.key, null, pack.preNodePortAdd, null, pack.initNode, QueryNature.DELETE, 1);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pack);
                    }
*/
//                    break;
//                }
//                delete(null, pack.key, new String[]{pack.initNode});
                break;
            default:
                Log.e("DYNAMO ::"+"" + TAG, "QueryNature Found: " + pack.QueryNature + "");
        }

    }
    private class AskForkeys extends AsyncTask<packet, Void, Void> {

        @Override
        protected Void doInBackground(packet... packobj) {
            packet p = packobj[0];
            boolean localExcept = false;
/*            int count = 0;
            while(count < 2)
            {
                count++;
*/
            Log.e("ASKFORKEYS ::","KEY ::"+p.key);
            for (Map.Entry<String, String> entry : NodeHashAddress.entrySet()) {
                String destAdd = entry.getValue();
                Socket socket;
                Log.e("DYNAMO ::" + "CLIENT_TASK ::", "I AM THE DESTINY ::" + destAdd + " KEY ::" + p.key + " VALUE ::" + p.value);
                Log.e("DYNAMO ::" + "CLIENT_TASK ::DEST ::", destAdd + "");
                Log.e("DYNAMO ::" + "CLIENT_TASK ::NAT ::", "" + p.QueryNature + "");
                Log.e("DYNAMO ::" + "CLIENT_TASK ::KEY ::", p.key + "");
                if (destAdd.equals(selfNodeID)) {
                    continue;
                }

                try {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * (Integer.parseInt(destAdd)));
                    socket.setSoTimeout(1000);
                    OutputStream s = socket.getOutputStream();
                    ObjectOutputStream obj = new ObjectOutputStream(s);
                    obj.writeObject(p);
                    obj.close();
                    socket.close();

//                    break;
                }catch(UnknownHostException e){
                    Log.e("DYNAMO ::" + "" + TAG, "CLIENT_TASK :: UN_KNOWN_HOST\n\n");
                    localExcept = true;
                    Log.e("DYNAMO ::" + "CLIENT_TASK ::", "FAILED TO SEND TO ::" + p.destiny);
                }catch(IOException e){
                    localExcept = true;
                    e.printStackTrace();
                    Log.e("DYNAMO ::" + "" + TAG, "CLIENT_TASK :: SOCKET IO EXCEPTION ::" + e.getCause() + "");
                    Log.e("DYNAMO ::" + "CLIENT_TASK ::", "FAILED TO SEND TO ::" + p.destiny);
                }
            }
            return null;
        }
    }
    //   //
    private class ClientTask extends AsyncTask<packet, Void, Void> {

        @Override
        protected Void doInBackground(packet... packobj) {
            packet p = packobj[0];
            boolean localExcept = false;
/*            int count = 0;
            while(count < 2)
            {
                count++;
*/              Socket socket;
            Log.e("DYNAMO ::"+"CLIENT_TASK ::","I AM THE DESTINY ::"+p.destiny+" KEY ::"+p.key+" VALUE ::"+p.value);
            Log.e("DYNAMO ::"+"CLIENT_TASK ::DEST ::", p.destiny + "");
            Log.e("DYNAMO ::"+"CLIENT_TASK ::NAT ::", "" + p.QueryNature + "");
            Log.e("DYNAMO ::"+"CLIENT_TASK ::KEY ::", p.key + "");

            try {

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * (Integer.parseInt(p.destiny)));
                OutputStream s = socket.getOutputStream();
                ObjectOutputStream obj = new ObjectOutputStream(s);
                obj.writeObject(p);
                obj.close();
                socket.close();
//                    break;
            } catch (UnknownHostException e) {
                Log.e("DYNAMO ::"+"" + TAG, "CLIENT_TASK :: UN_KNOWN_HOST\n\n");
                localExcept = true;
                Log.e("DYNAMO ::"+"CLIENT_TASK ::","FAILED TO SEND TO ::"+p.destiny);
            } catch (IOException e) {
                localExcept = true;
                e.printStackTrace();
                Log.e("DYNAMO ::"+"" + TAG, "CLIENT_TASK :: SOCKET IO EXCEPTION ::" + e.getCause() + "");
                Log.e("DYNAMO ::"+"CLIENT_TASK ::","FAILED TO SEND TO ::"+p.destiny);
            }

            // =============== IF SECOND SUCCESSOR DONT FORWARD TO NEXT ============== //
            if((p.QueryNature == QueryNature.INSERT || p.QueryNature == QueryNature.DELETE) && p.TTL == 1)
            {
                return null;
            }
            // ============== IF NEWLY RECOVERED NODE RE DIED DONT SEND ================ //
            if(p.QueryNature == QueryNature.RECEIVE_KEYS || p.QueryNature == QueryNature.REQUEST_KEYS)
            {
                Log.e("DYNAMO ::"+"CLIENT_TASK ::",":: THIS IS REQUEST OR RECEIVE CALL, NOT GOING FURTHER EVEN IF EXCEPTION");
                return null;
            }
            // ================ IF IT IS INSERT QUERY OR DELETE WITH NODE TTL 2 THIS WILL WORK TO REPLICATE ONLY TO NEXT NODE ================= //
            p.TTL--;
            if(p.TTL == -1)
            {
                p.TTL = 2;
            }
            p.destiny = getSuccOfNode(p.destiny);
//            }
            // =================== IF TTL IS -1 THEN PREVIOUS WAS THE KEY FOR THE BASE CASE SO INSERT IT IN SELF AND FORWARD WITH TTL -1 =================== //
            if(localExcept)
            {
                if(p.destiny.equals(selfNodeID))
                {
                    Log.e("DYNAMO ::"+"CLIENT_TASK ::","I AM FOUND EQUAL NATURE ::"+p.QueryNature+" TTL ::"+p.TTL);
                    if(p.QueryNature == QueryNature.INSERT)
                    {
                        if(p.TTL == 2) // IF THE OWNER DIES I AM THE SUCC
                        {
                            Log.e("DYNAMO ::"+"CLIENT_TASK ::", "I SHOULD HAVE SKIPPED BUT I WOULDN'T BECAUSE NOW I AM THE OWNER");
                            messgInfo messg = new messgInfo(p.value, neigh1);
                            Localstorage.put(p.key, messg);
                            p.TTL = 1;
                        }
                        else if(p.TTL == 1)
                        {
                            // DO NOTHING LET IT GO // THIS WILL NOT OCCUR
                            Log.e("DYNAMO ::"+"CLIENT_TASK ::","THIS SHOULD NEVER OCCUR 1");
                        }
                        else if(p.TTL == 0)
                        {
                            Log.e("DYNAMO ::"+"CLIENT_TASK ::","THIS SHOULD NEVER OCCUR 0");
                            return null;
                        }
                        p.destiny = neigh1;
                        p.TTL = 1;
                        Log.e("DYNAMO ::"+"CLIENT_TASK ::","PROCEEDING TO SEND WITH TTL 1 TO SUCC ::"+neigh1);
                    }
                    else if(p.QueryNature == QueryNature.DELETE)
                    {
                        if(p.TTL == 2)
                        {
                            Localstorage.remove(p.key);
                        }
                        p.TTL = 1;
                        p.destiny = neigh1;

                    }
                    else if(p.QueryNature == QueryNature.QUERY)
                    {
                        if(!selfNodeID.equals(p.initNode)) {
                            globalPacketStack.key = p.key;
                            globalPacketStack.value = p.value;
                            globalPacketStack.DBcursor.putAll(p.DBcursor);
                            wait = false;

                            synchronized (globalPacketStack) {
                                globalPacketStack.notify();
                            }

                            Log.e("DYNAMO ::"+"CLIENT_TASK ::", "QUERY FAIL ::IT WAS KEY ::" + p.key);
                            p = new packet(p.initNode, p.key, Localstorage.get(p.key).value, p);
                            Log.e("DYNAMO ::CLIENT_TASK ::", "QUERY FAIL I AM SUCC:: RETURN QUERY RESULT TO BOSS\n\n");
//                           whose_pack(p);
                            return null;
                        }
                        else
                        {
                            whose_pack(p);
                            return null;
                        }
                    }
                    else
                    {
                        Log.e("DYNAMO ::"+"CLIENT_TASK ::","THIS IS MY ADDRESS ::ESCAPE");
                        return null;
                    }
                }
                // ******************** IF KEY WAS NOT MY KEY AND TTL IS -1 SO NOT MY PREVIOUS PASS TO SUCC TO INSERT ********************* //
                if(p.TTL == -1)
                {
                    p.TTL = 2;
                }
                Log.e("DYNAMO ::"+"CLIENT_TASK ::","SECOND_CALL NEW DESTINY ::"+p.destiny);
                Log.e("DYNAMO ::"+"CLIENT_TASK ::","I AM THE DESTINY ::"+p.destiny+" KEY ::"+p.key+" VALUE ::"+p.value);
                Log.e("DYNAMO ::"+"CLIENT_TASK ::DEST ::", p.destiny + "");
                Log.e("DYNAMO ::"+"CLIENT_TASK ::NAT ::", "" + p.QueryNature + "");
                Log.e("DYNAMO ::"+"CLIENT_TASK ::KEY ::", p.key + "");

                try {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * (Integer.parseInt(p.destiny)));
                    OutputStream s = socket.getOutputStream();
                    ObjectOutputStream obj = new ObjectOutputStream(s);
                    obj.writeObject(p);
                    obj.close();
                    socket.close();
//                    break;
                } catch (UnknownHostException e) {
                    Log.e("DYNAMO ::"+"" + TAG, "CLIENT_TASK :: UN_KNOWN_HOST\n\n");
                    exception = true;
                } catch (IOException e) {
                    exception = true;
                    e.printStackTrace();
                    Log.e("DYNAMO ::"+"" + TAG, "CLIENT_TASK :: SOCKET IO EXCEPTION ::" + e.getCause() + "");
                }

            }

            return null;
        }

    }
}
class packet implements Serializable {
    private static final long serialVersionUID = 789456345456678789L;

    public String destiny;
    public String key;
    public String value;
    public String preNodePortAdd;
    public String succNodePrtAdd;
    public String initNode;
    public int TTL;
    public QueryNature QueryNature;
    public HashMap<String, messgInfo> DBcursor;

    public boolean TIMEOUT;

    public packet() {
        destiny = "";
        key = "";
        value = "";
        preNodePortAdd = "";
        succNodePrtAdd = "";
        initNode = "";
        DBcursor = new HashMap<String, messgInfo>();
        TIMEOUT = false;
        TTL = 0;
    }

    public packet(packet pckt) {
        this();
        assign(pckt);
    }

    public packet(String key, String initNode, QueryNature nature) {
        this();
        this.key = key;
        this.initNode = initNode;
        this.QueryNature = nature;
    }

    public packet(String destiny, packet pack, boolean timeout) {
        this(pack);
        this.destiny = destiny;
        this.TIMEOUT = timeout;
        Log.e("DYNAMO ::"+"PACKET :: DESTINY::", destiny + "");
    }

    public packet(String destiny, String key, String value, packet pckt) {
        this(pckt);
        this.destiny = destiny;
        this.key = key;
        this.value = value;

    }

    public packet(String destPortAdd, String key, String value, String preNodePortAdd, String succNodePrtAdd, String initNode, QueryNature QueryNature, int ttl) {
        this();
        this.destiny = destPortAdd;
        this.key = key;
        this.value = value;
        this.preNodePortAdd = preNodePortAdd;
        this.succNodePrtAdd = succNodePrtAdd;
        this.QueryNature = QueryNature;
        this.initNode = initNode;
        this.TTL = ttl;
    }

    public void assign(packet pckt) {
        this.destiny = pckt.destiny;
        this.preNodePortAdd = pckt.preNodePortAdd;
        this.succNodePrtAdd = pckt.succNodePrtAdd;
        this.key = pckt.key;
        this.value = pckt.value;
        this.QueryNature = pckt.QueryNature;
        this.DBcursor = pckt.DBcursor;
        this.initNode = pckt.initNode;
    }
}

class messgInfo implements Serializable {
    private static final long serialVersionUID = 789043215673678435L;
    public String value;
    public String port;

    public messgInfo()
    {
        this.value = "";
        this.port = "";
    }
    public messgInfo(String value, String port)
    {
        this.value = value;
        this.port = port;
    }
}

enum QueryNature {INSERT, QUERY, DELETE, REQUEST_KEYS, RECEIVE_KEYS}
