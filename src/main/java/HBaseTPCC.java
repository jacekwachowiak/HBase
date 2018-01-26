import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;


public class HBaseTPCC {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private byte[] tableName = Bytes.toBytes("Customer");
    private byte[] columnFamilyCustomerInfo = Bytes.toBytes("CustomerInfo");
    private String folder;

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseTPCC(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);

    }

    public void createTPCCTables() throws IOException {
        //TO IMPLEMENT
//        System.exit(-1);
        HTableDescriptor table = new
                HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor family = new
                HColumnDescriptor(columnFamilyCustomerInfo);
        family.setMaxVersions(6); // Default is 3.
        table.addFamily(family);
        hBaseAdmin.createTable(table);
    }

    public void loadTables(String folder)throws IOException{
        //We should load the data with the key WarehouseId (16 bytes?), DistrictId (16 bytes?), customerId (16 bytes?)
        //as the column we need customer_id, date and discount, district_id.

        //this is a csv printout to test
//        Scanner scanner = new Scanner(new File(folder + "/warehouse.csv"));
//        scanner.useDelimiter(",");
//        while(scanner.hasNext()){
//            System.out.println(scanner.next());
//        }
//        scanner.close();

        HTable table = new HTable(config, tableName);
        String csvFile = folder + "/customer.csv";
        String csvFile2 = folder + "/orders.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        System.out.println("Start loading files \n\n");

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                String[] row = line.split(cvsSplitBy);
                try {
                    List<Put> puts = new ArrayList<Put>();

                    //with our key, works too

//                    String[] keys = new String[] {row[2], row[1], row[0]};
//                    Put put = new Put(getKey2(keys));
//                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("customer_id"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[0]));
//                    puts.add(put);
//                    put = new Put(getKey2(keys));
//                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("discount"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[15]));
//                    puts.add(put);
//                    put = new Put(getKey2(keys));
//                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("district_id"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[1]));
//                    puts.add(put);

                    String[] keys = new String[] {row[2], row[1], row[0]};
                    int[] keyTable = new int[3];
                    keyTable[0] = 0;
                    keyTable[1] = 1;
                    keyTable[2] = 2;
                    byte[] myKey = getKey(keys, keyTable);
                    Put put = new Put(myKey);
                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("customer_id"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[0]));
                    puts.add(put);
                    put = new Put(myKey);
                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("discount"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[15]));
                    puts.add(put);
                    put = new Put(myKey);
                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("district_id"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[1]));
                    puts.add(put);

//                    //if you want the customer name not ID just add another column and then ask for it in the query - follow comments
//                    put = new Put(getKey2(keys));
//                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("customer_name"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[5]));
//                    puts.add(put);
//                    System.out.println("put2 \n\n\n\n\n");

                    table.put(puts);
                } finally {

                    if (table != null) {

                        table.flushCommits();
                    }
                }


            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //
        try {
            br = new BufferedReader(new FileReader(csvFile2));
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                try {
                    List<Put> puts = new ArrayList<Put>();

                    String[] keys = new String[] {row[2], row[1], row[3]};
                    int[] keyTable = new int[] {0, 1, 2};
                    byte[] myKey = getKey(keys, keyTable);
                    Put put = new Put(myKey);
                    put.add(columnFamilyCustomerInfo, Bytes.toBytes("date"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes(row[4]));
                    puts.add(put);


                    table.put(puts);
                } finally {
                    if (table != null) {
                        table.flushCommits();
                    }
                }


            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("End loading files \n\n");

    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
//            System.out.println("check  " + values[keyId]);
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }

    //our key with fixed sizes, works too
//    private byte[] getKey2(String[] values) {
//        byte[] key = new byte[48];
//        for (int i = 0; i < values.length; i++)
//            System.arraycopy(Bytes.toBytes(values[i]), 0, key,
//                    16 * i, values[i].length());
//
////        String keyString = "";
////        for (String value : values){
////            keyString += value;
////        }
////        byte[] key = Bytes.toBytes(keyString);
////
//        return key;
//    }

//    byte[] generateStartKey(String[] values) {
//        byte[] key = new byte[48];
//        for (int i = 0; i < values.length; i++)
//            System.arraycopy(Bytes.toBytes(values[i]), 0, key,
//                    16 * i, values[i].length());
//        for (int i = 16 * values.length; i < 48; i++){
//            key[i] = (byte)-255;
//        }
//        return key;
//    }
//    byte[] generateEndKey(String[] values) {
//        byte[] key = new byte[48];
//        for (int i = 0; i < values.length; i++)
//            System.arraycopy(Bytes.toBytes(values[i]), 0, key,
//                    16 * i, values[i].length());
//        for (int i = 16 * values.length; i < 48; i++){
//            key[i] = (byte)255;
//        }
//        return key;
//    }

    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {
        HTable table = new HTable(config, tableName);
        List<Filter> filters = new ArrayList<Filter>();
        Filter f = new SingleColumnValueFilter(columnFamilyCustomerInfo, Bytes.toBytes("date"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDate));
        Filter f2 = new SingleColumnValueFilter(columnFamilyCustomerInfo, Bytes.toBytes("date"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endDate));
        filters.add(f);
        filters.add(f2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        String[] startKey = new String[] {warehouseId, districtId, "1"};
        String[] endKey = new String[] {warehouseId, districtId, "9999"};
        int[] keyTable = new int[] {0, 1, 2};

        Scan scan = new Scan(getKey(startKey, keyTable), getKey(endKey, keyTable));
        scan.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("customer_id")); //customer_name if the name wanted - uncomment the column addition before
        scan.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("date"));
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        Result result = resultScanner.next();
        List<String> output = new ArrayList<String>();
        while( result != null && ! result.isEmpty()){
            //Only customerId. We can extend it if we want

            String row = new String(result.value());
            output.add(row);
            result = resultScanner.next();
        }
        return output;

    }

//    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
//        HTable table = new HTable(config, tableName);
//        String[] values = new String[3];
//        values[0] = warehouseId;
//        values[1] = districtId;
//        values[2] = customerId;
//        byte[] key = getKey2(values);
//        //List<Put> puts = new ArrayList<>();
//
//        for (String discount : discounts) {
//            Put put = new Put(key);
//            put.add(columnFamilyCustomerInfo, Bytes.toBytes("discount"), Bytes.toBytes(discount));
//            table.put(put);
//        }
//
//
//    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
        HTable table = new HTable(config, tableName);
        String[] values = new String[3];
        values[0] = warehouseId;
        values[1] = districtId;
        values[2] = customerId;
        int[] keyTable = new int[] {0, 1, 2};
        byte[] key = getKey(values, keyTable);
        if (discounts.length >6)
        {
            System.out.println("You have given more than 6 discounts, only the first 6 will be used, to add more use the query again \n");
        }
        Integer discountCount = 1;
        for (String discount : discounts) {
            if (discountCount<7) {
                Put put = new Put(key);
                put.add(columnFamilyCustomerInfo, Bytes.toBytes("discount"), Bytes.toBytes(discount));
                table.put(put);
            }
            discountCount++;
        }
        System.out.println("Added\n");

    }

//    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
//        HTable table = new HTable(config, tableName);
//        String[] values = new String[3];
//        values[0] = warehouseId;
//        values[1] = districtId;
//        values[2] = customerId;
//        byte[] key = getKey2(values);
//
//        Get get = new Get(key);
//        get.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("discount"));
//        get.setMaxVersions(4);
//        Result result = table.get(get);
//
//        List<String> output = new LinkedList<>();
//        Cell current = result.current();
//        while (result != null && !result.isEmpty() && result.advance())
//        {
//            current = result.current();
//            output.add(new String(Bytes.toString(CellUtil.cloneValue(current))));
//        }
//
//            String[] arrayOutput = new String[output.size()];
//        return output.toArray(arrayOutput);
//
//    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
        HTable table = new HTable(config, tableName);
        String[] values = new String[3];
        values[0] = warehouseId;
        values[1] = districtId;
        values[2] = customerId;
        int[] keyTable = new int[] {0, 1, 2};
        byte[] key = getKey(values, keyTable);

        Get get = new Get(key);
        get.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("discount"));
        get.setMaxVersions(4);
        Result result = table.get(get);

        List<String> output = new LinkedList<>();
        Cell current = result.current();

        while (result != null && !result.isEmpty() && result.advance())
        {
            current = result.current();
            output.add(new String(Bytes.toString(CellUtil.cloneValue(current))));
        }

        String[] arrayOutput = new String[output.size()];
        return output.toArray(arrayOutput);

    }

    //approach with filter

//    public List<Integer>  query4(String warehouseId, String[] districtIds) throws IOException {
//
//        HTable table = new HTable(config, tableName);
//        Scan scan = new Scan(generateStartKeyShort(warehouseId), generateEndKeyShort(warehouseId));
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        for (String i : districtIds)
//        {
//            filterList.addFilter(new SingleColumnValueFilter(columnFamilyCustomerInfo, Bytes.toBytes("district_id"),  CompareFilter.CompareOp.EQUAL, Bytes.toBytes(i)));
//        }
//
//        scan.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("customer_id"));
//        scan.setFilter(filterList);
//        ResultScanner resultScanner = table.getScanner(scan);
//        Result result = resultScanner.next();
//
//        List<Integer> output = new ArrayList<Integer>();
//        while( result != null && ! result.isEmpty()){
//            //Only customerId. We can extend it if we want
//            String row = new String(result.value());
//            output.add(Integer.parseInt(row));
//            result = resultScanner.next();
//        }
//        return output;
//    }

    public List<Integer>  query4(String warehouseId, String[] districtIds) throws IOException {

        HTable table = new HTable(config, tableName);
//        String[] values = new String[2];
//        values[0] = warehouseId;
        List<Integer> output = new ArrayList<Integer>();
        for (String district : districtIds) {
//            values[1] = district;
            String[] startKey = new String[] {warehouseId, district, "0"};
            String[] endKey = new String[] {warehouseId, district, "9999"}; //necessary since not sorted
            int[] keyTable = new int[] {0, 1, 2};
            Scan scan = new Scan(getKey(startKey, keyTable), getKey(endKey, keyTable));
            scan.addColumn(columnFamilyCustomerInfo, Bytes.toBytes("customer_id"));
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();

            while (result != null && !result.isEmpty()) {
                //Only customerId. We can extend it if we want
                String row = new String(result.value());
                output.add(Integer.parseInt(row));
                result = resultScanner.next();
            }
        }
        return output;
    }

    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTables, loadTables, query1, query2, query3, query4], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTables: csvsFolder.\n " +
                    "\tb) If query1: warehouseId districtId startDate endData.\n  " +
                    "\tc) If query2: warehouseId districtId customerId listOfDiscounts.\n  " +
                    "\td) If query3: warehouseId districtId customerId.\n  " +
                    "\te) If query4: warehouseId listOfdistrictId.\n  ");
            System.exit(-1);
        }
        HBaseTPCC hBaseTPCC = new HBaseTPCC(args[0]);

        if(args[1].toUpperCase().equals("CREATETABLES")){
            hBaseTPCC.createTPCCTables();
        }
        else if(args[1].toUpperCase().equals("LOADTABLES")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }

            hBaseTPCC.loadTables(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) warehouseId 4) districtId 5) startDate 6) endData");
                System.exit(-1);
            }

            List<String> customerIds = hBaseTPCC.query1(args[2], args[3], args[4], args[5]);
            System.out.println("There are "+customerIds.size()+" customers that order products from warehouse "+args[2]+" of district "+args[3]+" during after the "+args[4]+" and before the "+args[5]+".");
            System.out.println("The list of customers is: "+Arrays.toString(customerIds.toArray(new String[customerIds.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) warehouseId 4) districtId 5) customerId 6) listOfDiscounts");
                System.exit(-1);
            }
            hBaseTPCC.query2(args[2], args[3], args[4], args[5].split(","));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=5){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) districtId 5) customerId");
                System.exit(-1);
            }
            String[] discounts = hBaseTPCC.query3(args[2], args[3], args[4]);
            System.out.println("The last 4 discounts obtained from Customer "+args[4]+" of warehouse "+args[2]+" of district "+args[3]+" are: "+Arrays.toString(discounts));
        }
        else if(args[1].toUpperCase().equals("QUERY4")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query4, " +
                        "3) warehouseId 4) listOfDistrictIds");
                System.exit(-1);
            }
            System.out.println("There are "+hBaseTPCC.query4(args[2], args[3].split(","))+" customers that belong to warehouse "+args[2]+" of districts "+args[3]+".");
        }
        else{
            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables, query1, query2, query3, query4], 3)Extra parameters for loadTables and queries:" +
                    "a) If loadTables: csvsFolder." +
                    "b) If query1: warehouseId districtId startDate endData" +
                    "c) If query2: warehouseId districtId customerId listOfDiscounts" +
                    "d) If query3: warehouseId districtId customerId " +
                    "e) If query4: warehouseId listOfdistrictId");
            System.exit(-1);
        }

    }



}
