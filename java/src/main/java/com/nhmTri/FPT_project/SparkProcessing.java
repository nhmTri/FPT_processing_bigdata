package com.nhmTri.FPT_project;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkProcessing implements Serializable {
    private List<Map<String, String>> fileList;
    private SparkSession spark;
    private List<String> dateList;
    private Dataset<Row> FinalDf;

    public SparkProcessing(List<Map<String, String>> fileList) {
        this.spark = SparkSessionProvider.getInstance();
        this.fileList = fileList;
    }

    // Create DataFrame and add a column use function lit to note the day of each day
    public Dataset<Row> createDataFrame(List<Map<String, String>> fileList) {
        Dataset<Row> intputDataFrame = null;
        for (Map<String, String> file : fileList) {
            String path = file.get("path");
            String dateSt = file.get("date");
            Dataset<Row> df = spark.read().json(path).withColumn("Date", to_date(lit(dateSt), "yyyyMMdd"));
            System.out.println("Reading file: " + path + " with Date: " + dateSt);
            df.show(1);
            if (intputDataFrame == null) {
                intputDataFrame = df;
            } else {
                intputDataFrame = intputDataFrame.unionByName(df);
            }
        }
        return intputDataFrame;
    }

    ;

    public Dataset<Row> extractingColumns(@NotNull Dataset<Row> inputDataframe) {
        Dataset<Row> returnedDf;
        returnedDf = inputDataframe.select(
                col("_source.Contract").alias("Contract"),
                col("_source.Mac").alias("Mac"),
                col("_source.TotalDuration").alias("TotalDuration"),
                col("_source.AppName").alias("AppName"),
                col("Date").alias("Date")
        );
        return returnedDf;
    }

    public Dataset<Row> dataFrameTransform(@NotNull Dataset<Row> returnedDf) {
        System.out.println("Start naming the columns from the type");
        System.out.println("----------------Processing----------------");
        Dataset<Row> finalDf = returnedDf.withColumn("Type",
                when(col("AppName").equalTo("CHANNEL")
                        .or(col("AppName").equalTo("DSHD"))
                        .or(col("AppName").equalTo("KPLUS")), "Television")
                        .when(col("AppName").equalTo("VOD")
                                .or(col("AppName").equalTo("FIMS_RES"))
                                .or(col("AppName").equalTo("BHD_RES"))
                                .or(col("AppName").equalTo("VOD_RES"))
                                .or(col("AppName").equalTo("FIMS"))
                                .or(col("AppName").equalTo("BHD"))
                                .or(col("AppName").equalTo("DANET")), "Film")
                        .when(col("AppName").equalTo("RELAX"), "Entertainment")
                        .when(col("AppName").equalTo("CHILD"), "Children")
                        .when(col("AppName").equalTo("SPORT"), "Sport")
                        .otherwise("Error")
        );

        Column TelevisionDuration = sum(
                when(col("Type").equalTo("Television"), col("TotalDuration"))
                        .otherwise(lit(0))
        ).alias("TelevisionDuration");

        Column FilmDuration = sum(
                when(col("Type").equalTo("Film"), col("TotalDuration"))
                        .otherwise(lit(0))
        ).alias("FilmDuration");

        Column EntertainmentDuration = sum(
                when(col("Type").equalTo("Entertainment"), col("TotalDuration"))
                        .otherwise(lit(0))
        ).alias("EntertainmentDuration");

        Column ChildrenDuration = sum(
                when(col("Type").equalTo("Children"), col("TotalDuration"))
                        .otherwise(lit(0))
        ).alias("ChildrenDuration");

        Column SportDuration = sum(
                when(col("Type").equalTo("Sport"), col("TotalDuration"))
                        .otherwise(lit(0))
        ).alias("SportDuration");

        finalDf = finalDf.groupBy("Date", "Contract")
                .agg(TelevisionDuration, FilmDuration, EntertainmentDuration, ChildrenDuration, SportDuration);

        return finalDf;
    }

    public Dataset<Row> gettingStasteUDF(@NotNull Dataset<Row> finalDf) {

        Column structCols = struct(
                finalDf.col("TelevisionDuration"),
                finalDf.col("FilmDuration"),
                finalDf.col("EntertainmentDuration"),
                finalDf.col("ChildrenDuration"),
                finalDf.col("SportDuration")
        );
        UDF1<Row, String> gettingTasteUDF = new UDF1<Row, String>() {
            ArrayList<String> resultList;

            @Override
            public String call(Row structRows) throws Exception {
                resultList = new ArrayList<>();
                long TelevisionValue = structRows.getLong(0);
                long FilmValue = structRows.getLong(1);
                long EntertainmentValue = structRows.getLong(2);
                long ChildrenValue = structRows.getLong(3);
                long SportValue = structRows.getLong(4);

                if (TelevisionValue > 0) resultList.add("TV");
                if (FilmValue > 0) resultList.add("Film");
                if (EntertainmentValue > 0) resultList.add("EntertainMent");
                if (ChildrenValue > 0) resultList.add("Children");
                if (SportValue > 0) resultList.add("Sport");

                return resultList.isEmpty() ? "Nothing" : String.join(" - ", resultList);
            }
        };
        spark.udf().register("gettingTaste", gettingTasteUDF, DataTypes.StringType);
        finalDf = finalDf.withColumn("Taste", callUDF("gettingTaste", structCols));
        return finalDf;
    }

    public Dataset<Row> gettingActiveness(Dataset<Row> inputDataframe) {
        System.out.println("Start getting the activeness number");
        System.out.println("----------------Processing----------------");
        WindowSpec windowSpec = Window.partitionBy("Date", "Contract").orderBy("Date");
        inputDataframe = inputDataframe.withColumn("TotalActiveness", count("Contract").over(windowSpec));
        return inputDataframe;
    }




};




