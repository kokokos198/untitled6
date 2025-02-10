package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import javax.xml.crypto.Data;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;

//jdbc:sqlserver://;serverName=192.168.10.249;databaseName=master

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws SQLException, IOException {

        Load loadData =  new Load();
        List<Dataset<Row>> df_list  = null;
        String path = "C:/Users/koshkinas/Desktop/prill2/untitled/src/main/resources/ts_gm.csv";
        String path_table = "C:/Users/koshkinas/Desktop/prill2/untitled/src/main/resources/str.txt";

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream("C:/Users/koshkinas/Desktop/prill2/untitled/src/main/resources/sql.properties")) {
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        try {
            Connection connection = DriverManager.getConnection(properties.getProperty("database.url"), properties.getProperty("database.login"), properties.getProperty("database.password"));

            df_list = loadData.rs_file(path_table).stream().map(i-> {
               try {
                   return loadData.df_render(i, connection, spark);
               } catch (SQLException e) {
                   throw new RuntimeException(e);
               }
           }).collect(toList());

        } catch (SQLException  e) {
            System.out.println(e.getMessage());
        }

        Dataset<Row> df_tinstitution      = df_list.get(0);
        Dataset<Row> df_tEntAttrValue     = df_list.get(1);
        Dataset<Row> df_tCntParams        = df_list.get(2);
        Dataset<Row> df_tDeal             = df_list.get(3);
        Dataset<Row> df_tInstRelation_BAK = df_list.get(4);
        Dataset<Row> df_tConfigParam      = df_list.get(5);
        Dataset<Row> df_tInstrument       = df_list.get(6);
        Dataset<Row> df_tObjClsRelation   = df_list.get(7);
        Dataset<Row> df_tObjClassifier    = df_list.get(8);

        Dataset<Row> tbrief = df_tEntAttrValue.as("atr").join(df_tCntParams.as("params"),
                col("atr.AttributeID").equalTo(col("params.CntParamsID")), "inner")
                .join(df_tinstitution.as("inst"), col("atr.ObjectID").equalTo(col("inst.InstitutionID")))
                .select("inst.InstitutionID", "atr.Value");


        Dataset<Row> df_t = df_tInstRelation_BAK.as("bak").join(df_tConfigParam.as("param"), col("bak.InstRelTypeID").equalTo(col("param.ID"))
                        .and(functions.trim(col("param.SysName")).isin("RATE_TRADING_PLATFORM")), "inner")
                        .join(df_tinstitution.as("inst"), col("inst.InstitutionID").equalTo(col("bak.InstParentID")))
                .select("bak.InstRelationID", "bak.Brief", "bak.DocName", "inst.InstitutionID");




        Dataset<Row> df1 = df_tDeal.as("d").join(df_t.as("t"), col("t.InstRelationID").equalTo(col("d.TradingSysID")), "inner")
                        .join(df_tInstrument.as("ins"), col("ins.InstrumentID").equalTo(col("d.InstrumentID")), "inner")
                                .join(df_tObjClsRelation.as("cls"), col("cls.ObjectID").equalTo(col("ins.InstrumentID")), "inner")
                                        .join(df_tObjClassifier.as("oc"), col("oc.ObjClassifierID").equalTo(col("cls.ObjClassifierID")), "inner")
                                                .join(tbrief.as("tb"), col("tb.InstitutionID").equalTo(col("t.InstitutionID")), "left")
                                                        .where(col("d.TradingSysID").notEqual(0))
                .select("d.TradingSysID",
                        "t.InstitutionID",
                        "tb.Value",
                        "t.DocName").distinct();

        Dataset<Row> df2 = df_tDeal.as("d").join(df_t.as("t"), col("t.InstRelationID").equalTo(col("d.TradingSysID")), "inner")
                        .join(df_tInstrument.as("ins"), col("ins.InstrumentID").equalTo(col("d.InstrumentID")), "inner")
                                .join(tbrief.as("tb"), col("tb.InstitutionID").equalTo(col("t.InstitutionID")), "left")
                                  .where(col("d.TradingSysID").notEqual(0))
                            .select("d.TradingSysID",
                                    "t.InstitutionID",
                                   "tb.Value",
                                    "t.DocName").distinct();

        Dataset<Row> df3 = df_tObjClassifier.as("oc").join(df_tObjClassifier.as("oc2"), col("oc2.ParentID").equalTo(col("oc.ObjClassifierID")))
                        .where(col("oc.ObjClassifierID").equalTo(10000166020L))
                .select(col("oc2.ObjClassifierID").as("TradingSysID")
                        , lit(null).as("InstitutionID")
                        , col("oc2.NAME").as("DocName")
                        , lit(null).as("Value"));

        Dataset<Row> df_itog = df1.union(df2).union(df3);

        Dataset<Row> df_itog2 = df_itog.select(
                when(col("Value").isNotNull(), "GMRDM_NDL_P_RDMGM")
                        .otherwise("GMRDM_NDL_DCS").as("operator_subj_dp_code"),
                when(col("Value").isNotNull(), col("Value"))
                        .otherwise(col("InstitutionID")).as("operator_subj_sk"),
                col("TradingSysID").as("sid"),
                lit("GMRDM_NDL_DCS").as(" ts_gm_dp_code"),
                col("DocName").as(" ts_gm_name"),
                col("TradingSysID").as(" ts_gm_sk")
        );

        List<String> list = df_itog2.collectAsList().stream()
                .map(r->r.toString())
                .map(str->str.substring(1, str.length() - 1))
                .collect(Collectors.toList());

        spark.close();

         Upload upload  = new Upload();
         upload.upload_file(list, path);

    }
}
