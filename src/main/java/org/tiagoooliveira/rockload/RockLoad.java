package org.tiagoooliveira.rockload;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;


public class RockLoad {
    private final static String ROCK_NAME = "rock-n-roll-db";

    public static void main(String [] args) {

        String csvfile = "../part-00000-63309610-d4c0-4a8e-97f9-ccce8667e598-c000.csv";
        String csvdelimiter = ",";
        String rockdbrepo = "/tmp/rocks";
        String deleterockdb = "True";
        int keycolumnindex = 0;
        int valuecolumnindex = 1;
        String onlycheckRockDBloadStats = "False";
        String batchorline = "Batch";

        if (args.length > 6)
        {
            csvfile = args[0];
            csvdelimiter = args[1];
            keycolumnindex = Integer.parseInt(args[2]);
            valuecolumnindex = Integer.parseInt(args[3]);
            batchorline = args[4];
            rockdbrepo = args[5];
            deleterockdb = args[6];
            onlycheckRockDBloadStats = args[7];
        }
        else
        {
            System.out.println("The set of arguments does not match the sequence expected. Should be i.e.: > rockdb.jar CSVFilePath CSVDelimiter KeyCSVColumnIndex ValueCSVColumnIndex BatchorLine RockDBRepository TrueFalseForDeleteDBAfterProcess TrueFalseForOnlyCheckRockDBloadStats");
            //only for debug mode
            //return;
        }

        System.out.println(String.format("Loading RockDB %s/%s", rockdbrepo, ROCK_NAME));

        Instant startloadingDB = Instant.now();
        File baseDir = new File(rockdbrepo, ROCK_NAME);
        long sizerecords = 0;

        RocksDB.loadLibrary();

        //https://github.com/facebook/rocksdb/wiki/Compression
        //https://www.ververica.com/blog/manage-rocksdb-memory-size-apache-flink
        //https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
        //https://www.arangodb.com/docs/stable/architecture-storage-engines.html
            //The RocksDB engine is optimized for large data-sets and allows for a steady insert performance even if the data-set is much larger than the main memory. Indexes are always stored on disk but caches are used to speed up performance.
            //RocksDB allows concurrent writes and reads
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try(final Options options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(200 * SizeUnit.MB) //maximum size for a MemTable
                .setMaxWriteBufferNumber(4) //the maximum number of â€œREAD ONLYâ€� MemTables in memory.
                .setMaxBackgroundCompactions(10)
                .setUnorderedWrite(true)
                .setMaxBackgroundFlushes(16)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL)){

            //investigate the behaviour with
            //  .setAllowConcurrentMemtableWrite(true)
            //  .setTwoWriteQueues(true)

            try {
                Files.createDirectories(baseDir.getParentFile().toPath());
                Files.createDirectories(baseDir.getAbsoluteFile().toPath());


                try (final RocksDB db = RocksDB.open(options, baseDir.getAbsolutePath())) {
                    System.out.println("RocksDB initialized");


                    Instant finishloadingDB = Instant.now();
                    long timeelapsedrock = Duration.between(startloadingDB, finishloadingDB).toMillis();
                    System.out.println(String.format("%d Millis timelapsed for loading RockDB" ,timeelapsedrock));


                    Instant startloadingfile = Instant.now();

                    if(onlycheckRockDBloadStats.compareToIgnoreCase("FALSE") == 0)
                    {
                        if(batchorline.compareToIgnoreCase("Batch") == 0)
                        {
                            sizerecords = loadCSV(csvfile, csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                        else {
                            sizerecords = loadCSVLine(csvfile, csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }


                    //for concurrent write & read tests
                    /*
                    Thread threadwriter1 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadWriter-1 initialized. Current time: " + sf.format(currentDate));
                            //loadCSV("../part-00000-64616384-dad9-4966-a419-1d0f3c7765a7-c000.csv", csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }, "ThreadWriter-1");
                    threadwriter1 .start();

                    Thread threadwriter2 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadWriter-2 initialized. Current time: " + sf.format(currentDate));
                            //loadCSV("../part-00000-63309610-d4c0-4a8e-97f9-ccce8667e598-c000.csv", csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }, "ThreadWriter-2");
                    threadwriter2 .start();


                    Thread threadwriter3 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadWriter-3 initialized. Current time: " + sf.format(currentDate));
                            //loadCSV("../test_nodes1.csv", csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }, "ThreadWriter-3");
                    threadwriter3.start();

                    Thread threadwriter4 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadWriter-4 initialized. Current time: " + sf.format(currentDate));
                            //loadCSV("../part-00000-41df0cb9-c8c6-4956-a226-01fd95c73dee-c000.csv", csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }, "ThreadWriter-4");
                    threadwriter4.start();

                    Thread threadwriter5 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadWriter-5 initialized. Current time: " + sf.format(currentDate));
                            //loadCSV("../part-00000-f62cdd4f-9f2d-4041-80d2-d1c7cab471ea-c000.csv", csvdelimiter, keycolumnindex, valuecolumnindex, db);
                        }
                    }, "ThreadWriter-5");
                    threadwriter5.start();

                    Thread threadreader1 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadReader-1 initialized. Current time: " + sf.format(currentDate));
                            findItems("../test_nodes1.csv", ",", db);
                        }
                    }, "ThreadReader-1");
                    threadreader1.start();

                    Thread threadreader2 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Date currentDate = new Date(System.currentTimeMillis());
                            final SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                            System.out.println("ThreadReader-2 initialized. Current time: " + sf.format(currentDate));
                            findItems("../part-00000-64616384-dad9-4966-a419-1d0f3c7765a7-c000.csv", ",", db);
                        }
                    }, "ThreadReader-2");
                    threadreader2.start();

                    try{
                        System.out.println("Syncing threads...");

                        Instant startsync = Instant.now();

                        threadwriter1.join();
                        threadwriter2.join();
                        threadwriter3.join();

                        threadwriter5.join();

                        threadwriter4.join();

                        threadreader2.join();
                        threadreader1.join();


                        Instant endsync = Instant.now();
                        long timeElapsedSync = Duration.between(startsync, endsync).toMillis();
                        System.out.println(String.format("%d Millis timelapsed for syncing writing threads" ,timeElapsedSync));

                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                    */

                    Instant finishloadingfile = Instant.now();
                    long timeelapsedfile = Duration.between(startloadingfile, finishloadingfile).toMillis();

                    String abspathDB = baseDir.getAbsoluteFile().toPath().toString();
                    System.out.println(String.format("%d Millis timelapsed for loading %d kv's. \nRockDB size on disk: %d" ,timeelapsedfile, sizerecords, dirSize(abspathDB)));

                    //Stats
                    System.out.println(String.format("rocksdb.size-all-mem-tables: %s" ,db.getProperty("rocksdb.size-all-mem-tables")));
                    System.out.println(String.format("rocksdb.cur-size-all-mem-tables: %s" ,db.getProperty("rocksdb.cur-size-all-mem-tables")));
                    System.out.println(String.format("rocksdb.estimate-num-keys: %s", db.getProperty("rocksdb.estimate-num-keys")));
                    System.out.println(String.format("rocksdb.estimate-table-readers-mem: %s", db.getProperty("rocksdb.estimate-table-readers-mem")));
                    System.out.println(String.format("rocksdb.stats: %s", db.getProperty("rocksdb.stats")));

                    /*
                    "rocksdb.size-all-mem-tables" => 1234436376 (1,2GB)
                    "rocksdb.cur-size-all-mem-tables" => 89890656 (0.1GB)
                    Pinned memtables is (IMO) the difference: => 1234436376 - 89890656 = 1.144.545.720 (1,1GB)*/

                } catch (RocksDBException e) {

                }
            } catch(IOException e){
                System.out.println(String.format("Error initializng RocksDB. Exception: %s, message: %s", e.getCause(), e.getMessage()));
            }

        }
        finally{
            try{
                if(deleterockdb.compareToIgnoreCase("TRUE") == 0)
                {
                    Files.walk(baseDir.getAbsoluteFile().toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);

                }
            } catch(IOException e){}
        }

    }

    private static long dirSize(String directory)
    {
        try {
            Path folder = Paths.get(directory);

            long size = Files.walk(folder)
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();

            return size;
        }catch(IOException e)
        {
            return 0;
        }
    }

    public static synchronized long loadCSV(String file, String delimiter, int keyindex, int valueindex, RocksDB db) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            try{
                //https://www.codota.com/code/java/classes/org.rocksdb.WriteBatch
                //https://www.programcreek.com/java-api-examples/?code=ljygz%2FFlink-CEPplus%2FFlink-CEPplus-master%2Fflink-state-backends%2Fflink-statebackend-rocksdb%2Fsrc%2Fmain%2Fjava%2Forg%2Fapache%2Fflink%2Fcontrib%2Fstreaming%2Fstate%2FRocksDBWriteBatchWrapper.java
                //https://rocksdb.org/blog/
                    //Write path, Performance characteristics
                //Setup capacity to flush batch into db
                WriteOptions writeOpt = new WriteOptions();
                WriteBatch batch = new WriteBatch(64);

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(delimiter);

                    try{
                        //batch.put(values[2].getBytes(StandardCharsets.UTF_8), values[0].getBytes(StandardCharsets.UTF_8));
                        batch.put(values[keyindex].getBytes(StandardCharsets.UTF_8), values[valueindex].getBytes(StandardCharsets.UTF_8));
                    } catch (RocksDBException e) {
                        System.out.println(String.format("Error building batch entries. Cause: %s, message: %s", e.getCause(), e.getMessage()));
                    }
                }

                //writing down batch
                db.write(writeOpt, batch);

                long count = batch.count();

                System.out.println(String.format("Thread %s loaded %d rows", Thread.currentThread().getName(), count));

                return count;
            } catch (RocksDBException e) {
                System.out.println(String.format("Error saving batch. Cause: %s, message: %s", e.getCause(), e.getMessage()));
                return 0;
            }
        }catch (FileNotFoundException e){
        }catch (IOException e){}
        return 0;
    }

    public static synchronized long loadCSVLine(String file, String delimiter, int keyindex, int valueindex, RocksDB db) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            long count = 0;

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(delimiter);

                    count ++;
                    try{
                        db.put(values[keyindex].getBytes(StandardCharsets.UTF_8), values[valueindex].getBytes(StandardCharsets.UTF_8));
                    } catch (RocksDBException e) {
                        System.out.println(String.format("Error building batch entries. Cause: %s, message: %s", e.getCause(), e.getMessage()));
                    }
                }

                System.out.println(String.format("Thread %s loaded %d rows", Thread.currentThread().getName(), count));

                return count;

        }catch (FileNotFoundException e){
        }catch (IOException e){}
        return 0;
    }

    public static synchronized void findItems(String file, String delimiter, RocksDB db) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            long count = 0;
            long max = 0;

            Instant startfileretrieval = Instant.now();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(delimiter);
                String key = values[0];

                count++;

                try {


                    Object value = null;

                    Instant startget = Instant.now();
                    byte[] bytes = db.get(key.getBytes(StandardCharsets.UTF_8));

                    Instant endget = Instant.now();

                    long timeElapsed = Duration.between(startget, endget).toMillis();
                    max = Math.max(timeElapsed, max);

                    if (bytes != null) value = new String(bytes, StandardCharsets.US_ASCII);
                } catch (RocksDBException e) {
                    System.out.println(String.format(
                            "Error retrieving the entry with key: %s, cause: %s, message: %s",
                            key,
                            e.getCause(),
                            e.getMessage()
                    ));
                }
            }

            Instant endfileretrieval = Instant.now();
            long timeElapsed = Duration.between(startfileretrieval, endfileretrieval).toMillis();
            float avg = timeElapsed/ (float)count;
            System.out.println(String.format("%d Millis timelapsed for retrieving %d KV-s. Avg retrieval each key %f Millis. Max time retrieval %d Millis. File KV-s %s" , timeElapsed, count, avg, max, file));


            //System.out.println(String.format("Thread %s loaded %d rows", Thread.currentThread().getName(), count));

        }catch (FileNotFoundException e){
        }catch (IOException e){}
    }


    public static synchronized Map<String, String> fromCSV(String file, String delimiter) {
        Map<String, String> kvlines = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(delimiter);

                kvlines.put(values[2], values[1]);
            }
        }catch (FileNotFoundException e){
        }catch (IOException e){}

        return kvlines;
    }


    public synchronized Optional<Object> find(String key, RocksDB db) {
        Object value = null;
        try {
            byte[] bytes = db.get(key.getBytes(StandardCharsets.UTF_8));
            if (bytes != null) value = new String(bytes, StandardCharsets.US_ASCII);
        } catch (RocksDBException e) {
            System.out.println(String.format(
                    "Error retrieving the entry with key: %s, cause: %s, message: %s",
                    key,
                    e.getCause(),
                    e.getMessage()
            ));
        }
        System.out.println(String.format("finding key %s returns %s", key, value));

        return value != null ? Optional.of(value) : Optional.empty();
    }
}

/*
https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
https://gist.github.com/pauca/c771ccc5ed869dc42b3127e77d73fca5#file-storerocksdb-scala
https://levelup.gitconnected.com/using-rocksdb-with-spring-boot-and-java-99cb1c43a834
https://dgraph.io/blog/post/badger-over-rocksdb-in-dgraph/
https://github.com/facebook/rocksdb/wiki/RocksDB-Users-and-Use-Cases
https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#parallelism-options
*/