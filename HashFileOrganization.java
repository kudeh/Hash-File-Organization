/**
 * Created by kudeh on 09/11/16.
 */
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;

public class HashFileOrganization {

    Map<Integer, Bucket> hashToBuckets;         //maps hashcodes to a bucket.
    long endOfFile = 0;                         //last byte written to file +1, represents where to create a new bucket at.
    public long  sizeOfBuckets = 1000;           //represents the number of bytes stored in a bucket.
    RandomAccessFile writer;

    /**
     * Bucket Inner Class.
     */
    public class Bucket{

        public long size;
        long startLocation;
        long bytesRemainig;
        public long recordCount;
        public  ArrayList<Long> slots;
        public long seekLocation;
        public  long toWrite;
        //public ArrayList<String> record;

        public Bucket(long startLocation){
            slots = new ArrayList<Long>();
            this.startLocation = startLocation;
            this.seekLocation = startLocation;
            recordCount = 0;
            size = sizeOfBuckets;
            bytesRemainig = sizeOfBuckets-19;
            //slots.add(this.startLocation);
            toWrite=(this.startLocation+this.size)-19;
            //record = new ArrayList<String>();
        }
    }

    /**
     * Constructor: Builds Initial.
     */
    public HashFileOrganization(File file){

        hashToBuckets = new HashMap<Integer, Bucket>();

        final long startTime = System.currentTimeMillis();

        //read records from file, using buffer of 8kiB
        try{
            writer = new RandomAccessFile("testhash.csv", "rw");                //Write Hash file in this file.
            //Open input stream test.txt for reading purpose.
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine(); //takes header file.


            //inserts all records in the dataset into the hash file.
            while((line = br.readLine()) != null){
                //System.out.println(line);

                insert(line);      //inserts each line into the Hash File Organization.
                //System.out.println(line);
            }

            //query
            query("BDL");
            query("JFK");

            delete("2007,1,1,1,1232,1225,1341,1340,WN,2891,N351,69,75,54,1,7,SMF,ONT,389,4,11,0,,0,0,0,0,0,0");
            delete("2007,1,25,4,1300,1240,1711,1655,AA,592,N3CXAA,191,195,168,16,20,DFW,BDL,1471,3,20,0,,0,16,0,0,0,0");
            delete("2007,1,31,3,1237,1238,1653,1651,AA,592,N3CGAA,196,193,177,2,-1,DFW,BDL,1471,6,13,0,,0,0,0,0,0,0");
            delete("2007,1,13,6,1342,1340,1701,1700,AA,636,N465AA,139,140,104,1,2,ORD,BOS,867,9,26,0,,0,0,0,0,0,0");
            delete("2007,1,10,3,1342,1340,1718,1700,AA,636,N4UBAA,156,140,121,18,2,ORD,BOS,867,4,31,0,,0,0,0,18,0,0");
            delete("2007,1,27,6,556,600,1140,1159,AA,634,N627AA,224,239,205,-19,-4,LAX,ORD,1745,6,13,0,,0,0,0,0,0,0");
            delete("2007,1,27,6,556,600,1140,1159,AA,634,AA,224,239,205,-19,-4,LAX,ORD,1745,6,13,0,,0,0,0,0,0,0");

            insert("2008,1,3,4,1323,1255,1526,1510,WN,4,N674AA,123,135,110,16,28,IND,TPA,838,4,9,0,,0,0,0,0,0,16");

            query("BDL");

            final long endTime = System.currentTimeMillis();
            System.out.println("Total execution time: " + ((endTime - startTime)/1000)/60 + " minutes!" );
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally {

        }
    }

    /**
     * hashCode(String key): Produces the hash code to determine the bucket the key would go in.
     */
    public int hashCode(String key){
        int hash = 7;

        for (int i = 0; i < key.length(); i++) {
            hash = hash*31 + key.charAt(i);
        }

        return hash;
    }

    /**
     * hasBucket(int hash): Finds if a buckets already exist for hash.
     */
    public boolean hasBucket(int hash){

        boolean found = false;

        Bucket i = hashToBuckets.get(hash);

        if(i != null)
            found = true;

        return found;
    }

    /**
     * insert(String record): Inserts a record into the file and the hash file organization.
     */
    public void insert(String record){
        //locate key to build hash organization around(say 22nd column)
        int index = ordinalIndexOf(record, ",", 17);//get to correct column for the Dest attribute.
        String dest = record.substring(index+1, index+4);
        // System.out.println(dest);

        try {

            int hash = hashCode(dest); //find hashCode for the dest attribute.

            Bucket bucket = hashToBuckets.get(hash);
            if (hasBucket(hash)) {                                  //If a  Bucket already exist for the hash.

                if(record.length() < bucket.bytesRemainig) {        //If the record can fit into the bucket(or its overflow)
                    writer.seek(bucket.seekLocation);               //seek to the buckets seek location.
                    writer.writeBytes(record + "\n");               //write the record at the seek location.
                    bucket.seekLocation += record.length() + 1;     //update the seek location to next free location.
                    bucket.bytesRemainig -= record.length()+1;        //decrement the space left.
                    bucket.recordCount += 1;
                }else{                                              //If the record doesn't fit in existing bucket.

                    Bucket b = new Bucket(endOfFile);               //create
                    createFileBucket(endOfFile);                    //create new bucket.
                    writer.seek(b.startLocation);
                    writer.writeBytes(record+"\n");
                    b.bytesRemainig-=record.length()+1;
                    b.seekLocation+=record.length()+1;              //update seek location
                    bucket.seekLocation = b.seekLocation;
                    bucket.bytesRemainig = b.bytesRemainig;
                    bucket.slots.add(b.startLocation);
                    writer.seek(bucket.toWrite);                    //the toWrite attribute is the value of the location->
                    bucket.toWrite=(b.startLocation+bucket.size)-19;//->to store the address of an overflow bucket.
                    writer.writeBytes("$"+b.startLocation);        //write 1 to represent an overflow and also the location of overflow.
                    bucket.recordCount += 1;
                }

            } else {                                                //else a bucket doesn't exist for hash.
                Bucket b = new Bucket(endOfFile);                   //create new bucket
                createFileBucket(endOfFile);
                writer.seek(b.startLocation);
                writer.writeBytes(record+"\n");                     //write record in bucket.
                b.slots.add(b.startLocation);
                b.bytesRemainig-=record.length()+1;
                b.seekLocation+=record.length()+1;
                b.recordCount += 1;

                hashToBuckets.put(hash, b);
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("INSERTED: "+record);
    }

    /**
     * delete(String record): Deletes a record from the file and the hash file organization.
     */
    public void delete(String record){
        int index = ordinalIndexOf(record, ",", 17);//get to correct column for the Dest attribute.
        String dest = record.substring(index+1, index+4);
        // System.out.println(dest);

        try {

            int hash = hashCode(dest); //find hashCode for the dest attribute.
            Bucket bucket = hashToBuckets.get(hash); //get the bucket in matches to.

            //writer.seek(bucket.startLocation);
            // System.out.println(bucket.startLocation);
            boolean found = false;
            boolean exist = false;
            long count = 0;
            long start = bucket.startLocation;
            long seekTo = start;

            writer.seek(seekTo);
            String rec = writer.readLine();
            count+=rec.length()+1;

            while(!found) {   // && count<=bucket.size
                if (rec.equals(record)) {
                    writer.seek(seekTo);
                    for(int i = 0; i < rec.length(); i++){
                        writer.writeBytes("#");
                    }
                    System.out.println("DELETED: " + record);
                    //System.out.println(seekTo);
                    found = true;
                    exist = true;
                } else if (rec.contains("$")) {
                    seekTo = Long.parseLong(rec.trim().replace("$","").replaceAll("/",""));
                    count = 0;
                    writer.seek(seekTo);
                    rec = writer.readLine();
                    //System.out.println(seekTo);
                }else {

                    seekTo+=rec.length()+1;
                    if(!(seekTo >= endOfFile)){
                        writer.seek(seekTo);
                        rec = writer.readLine();
                        count+=rec.length()+1;}else{
                        found = true;
                    }

                    //System.out.println(seekTo);
                }
            }

            if(!exist){
                System.out.println("!!--The Record Doesn't Exist--!!");
            }


        }catch (Exception e){
            e.printStackTrace();

        }

    }

    /**
     * query(Dest): Returns all records with the provided destination.
     */
    public void query(String dest){
        int hash = hashCode(dest); //find hashCode for the dest attribute.
        Bucket bucket = hashToBuckets.get(hash); //get the bucket in matches to.

        long start = bucket.startLocation;
        long seekTo = start;
        long count = 0;
        long found = 0;

        try {
            writer.seek(seekTo);
            String rec = writer.readLine();

            while(count <= bucket.recordCount) {

                int index = ordinalIndexOf(rec, ",", 17);//get to correct column for the Dest attribute.
                String str = rec.substring(index + 1, index + 4);

                if(dest.equals(str)){
                    //System.out.println(rec);
                    count+=1;
                    found+=1;
                    seekTo+=rec.length()+1;
                }else if (rec.contains("$")) {
                    seekTo = Long.parseLong(rec.trim().replace("$","").replaceAll("/",""));
                }else {
                    seekTo+=rec.length()+1;
                    count+=1;
                    //System.out.println(seekTo);
                }

                writer.seek(seekTo);
                rec = writer.readLine();
            }

            System.out.println("Found: "+found);
        }catch (Exception e){
            e.printStackTrace();
        }


    }

    /**
     * main(): reads a specified csv file.
     */
    public static void main(String[] args){
        new HashFileOrganization(new File("test.csv"));  //read csv file from here.
    }

    public int ordinalIndexOf(String str, String substr, int n) {
        int pos = str.indexOf(substr);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(substr, pos + 1);
        return pos;
    }

    public void createFileBucket(long position){
        try{
            writer.seek(position);
            int count = 0;
            for(long i = 0; i < sizeOfBuckets; i++){
                writer.writeBytes("/");
            }
            writer.writeBytes("\n");
            endOfFile+=sizeOfBuckets+1;
            // writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
