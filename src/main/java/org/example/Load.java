package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Load {
    public  Dataset<Row> df_render(String str, Connection connection, SparkSession spark) throws SQLException {

        String str1 = "select " + str.substring(str.indexOf(",") + 1, str.length()) + " from " + str.substring(0, str.indexOf(","));
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(str1, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<Row> list = rs_query(resultSet, str, connection);
        StructType struct_t = struct_type(resultSet);
        Dataset<Row> df = spark.createDataFrame(list, struct_t);
        return df;
    }

    public  StructType struct_type(ResultSet resultSet) throws SQLException {
        int count = resultSet.getMetaData().getColumnCount();
        StructField[] structFields = new StructField[count];
        if(resultSet.first())
        {
            for(int i = 0; i < count; i++)
            {
                structFields[i] = (new StructField(resultSet.getMetaData().getColumnName(i + 1).trim(),
                        resultSet.getMetaData().getColumnType(i + 1) == 2 ? DataTypes.IntegerType : DataTypes.StringType, true, Metadata.empty()));
            }
        }
        StructType structType = new StructType(structFields);
        return  structType;
    }

    public  List<Row> rs_query (ResultSet resultSet, String str, Connection connection) throws SQLException {
        List<Row> list = new ArrayList<>();
        while (resultSet.next())
        {
            int count  = resultSet.getMetaData().getColumnCount();
            Object[] array = new Object[count];
            for(int i = 1; i <= count; i++)
            {
                array[i -1] = resultSet.getMetaData().getColumnType(i) == 2 ? resultSet.getInt(i) : resultSet.getString(i).trim();
            }
            list.add(RowFactory.create(array));
        }
        return  list;
    }

    public  List<String> rs_file(String path_table)
    {
        List<String> str_list = new ArrayList<>();
        try
        {
            File file = new File(path_table);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String lin;
            while((lin = bufferedReader.readLine()) != null)
            {
                str_list.add(lin);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return  str_list;
    }
}
