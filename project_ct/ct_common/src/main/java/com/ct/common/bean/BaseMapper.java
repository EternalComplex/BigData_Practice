package com.ct.common.bean;

import com.ct.common.api.Column;
import com.ct.common.api.RowKey;
import com.ct.common.api.TableRef;
import com.ct.common.constant.Names;
import com.ct.common.util.DateUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseMapper {
    private final ThreadLocal<Connection> connHolder = new InheritableThreadLocal<Connection>();
    private final ThreadLocal<Admin> adminHolder = new InheritableThreadLocal<Admin>();

    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }

    protected void end() throws IOException {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }

        Connection connection = getConnection();
        if (connection != null) {
            connection.close();
            connHolder.remove();
        }
    }

    // 创建命名空间，若存在，则不需要创建
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();

        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            // 出现异常说明没有该命名空间，在此处创建命名空间
            NamespaceDescriptor build = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(build);
        }
    }

    // 创建表，如果表已经存在，则删除后再创建(不分区)
    protected void createTableXX(String namespace, String tableName, String... columnFamilies) throws IOException {
        createTableXX(namespace, tableName, null, null, columnFamilies);
    }

    // 创建表，如果表已经存在，则删除后再创建(分区)
    protected void createTableXX(String namespace, String tableName, String coprocessorClass, Integer regionCount, String... columnFamilies) throws IOException {
        Admin admin = getAdmin();

        TableName tn = TableName.valueOf(namespace, tableName);
        // 若存在，则删除
        if (admin.tableExists(tn)) deleteTable(namespace, tableName);

        // 创建表格,若没有传递列族参数，传递默认参数
        if (columnFamilies == null || columnFamilies.length == 0) {
            columnFamilies = new String[1];
            columnFamilies[0] = Names.CF_INFO.getValue();
        }
        createTable(namespace, tableName, coprocessorClass, regionCount, columnFamilies);
    }

    private void createTable(String namespace, String tableName, String coprocessorClass, Integer regionCount, String... columnFamilies) throws IOException {
        Admin admin = getAdmin();

        TableName tn = TableName.valueOf(namespace, tableName);

        // 创建表格描述的建造者对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        for (String columnFamily : columnFamilies) {
            // 创建列族描述的建造者对象
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            columnFamilyDescriptorBuilder.setMaxVersions(5);    // 设置版本参数
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        // 设置协处理器用于添加被叫用户数据（具体逻辑在ct_consumer_coprocessor项目中）
        if (coprocessorClass != null && !"".equals(coprocessorClass)) {
            tableDescriptorBuilder.setCoprocessor(coprocessorClass);
        }

        // 创建表格
        if (regionCount == null || regionCount <= 1) {  // 不需要分区
            admin.createTable(tableDescriptorBuilder.build());
        } else { // 增加预分区
            // 分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptorBuilder.build(), splitKeys);
        }
    }

    // 生成分区键
    private byte[][] genSplitKeys(Integer regionCount) {
        int splitKeyCount = regionCount - 1;    // n个分区键可分n+1个分区
        byte[][] bs = new byte[splitKeyCount][];
        // 0|,1|,2|,3|,4|
        // (-∞, 0|), [0|, 1|), [1|, ...], (..., +∞)
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

//        Collections.sort(bsList, new Bytes.ByteArrayComparator());

        bsList.toArray(bs);

        return bs;
    }

    // 获取查询时的startRow，stopRow集合
    protected List<String[]> getStartStopRowKeys(String tel, String start, String end) {
        List<String[]> rowKeys = new ArrayList<String[]>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        // 创建日历对象
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));
        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {
            // 当前时间
            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

            int regionNum = genRegionNum(tel, nowTime);

            // 1_176_202303 ~ 1_176_202303|
            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String endRow = startRow + "|";

            rowKeys.add(new String[]{startRow, endRow});

            // 月份+1
            startCal.add(Calendar.MONTH, 1);
        }

        return rowKeys;
    }

    // 计算分区号(根据电话号码和时间计算)
    protected int genRegionNum(String tel, String date) {
        // 176 + 3954 + 1473    选取没有规律的部分
        String userCode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);

        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc校验采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum = crc % Integer.parseInt(Names.REGION_COUNT.getValue());

        return regionNum;
    }

    // 向表中添加对象（自动封装对象，将对象数据保存到hbase）
    protected void putData(Object obj) throws IOException, IllegalAccessException {
        // 反射
        Class<?> clazz = obj.getClass();
        // 通过注解获取值（表名）
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        // 通过注解获取属性
        String strRowKey = "";
        for (Field field : clazz.getDeclaredFields()) {
            RowKey rowKey = field.getAnnotation(RowKey.class);
            if (rowKey != null) {
                field.setAccessible(true);  // 允许获取私有属性
                strRowKey = (String) field.get(obj);
                break;
            }
        }

        // 获取table
        Table table = getConnection().getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(strRowKey));
        // 添加列族描述
        for (Field field : clazz.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String family = column.family();    // 列族
                String colName = column.column();   // 列
                if ("".equals(colName) || colName == null) colName = field.getName();
                field.setAccessible(true);          // 值
                String value = (String) field.get(obj);

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }

        // 添加数据
        table.put(put);

        // 关闭table
        table.close();
    }

    // 向表中添加数据
    protected void putData(String namespace, String tableName, Put put) throws IOException {
        TableName tn = TableName.valueOf(namespace, tableName);
        // 获取table
        Table table = getConnection().getTable(tn);

        // 添加数据
        table.put(put);

        // 关闭table
        table.close();
    }

    // 删除表
    protected void deleteTable(String namespace, String tableName) throws IOException {
        Admin admin = getAdmin();

        TableName tn = TableName.valueOf(namespace, tableName);
        admin.disableTable(tn);
        admin.deleteTable(tn);
    }

    // 获取连接对象
    protected Connection getConnection() throws IOException {
        Connection connection = connHolder.get();
        if (connection == null) {
            connection = ConnectionFactory.createConnection();   // 自动读取hbase-site.xml的配置
            connHolder.set(connection);
        }

        return connection;
    }

    // 获取admin
    protected Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();   // 自动读取hbase-site.xml的配置
            adminHolder.set(admin);
        }

        return admin;
    }
}
