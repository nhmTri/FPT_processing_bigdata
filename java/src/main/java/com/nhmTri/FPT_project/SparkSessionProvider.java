package com.nhmTri.FPT_project;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {
    private static volatile SparkSession instance = null;

    private SparkSessionProvider() {
        // ngăn không cho tạo object từ bên ngoài
    }

    public static SparkSession getInstance() {
        if (instance == null) {
            synchronized (SparkSessionProvider.class) {
                if (instance == null) {
                    instance = SparkSession.builder()
                            .appName("My Spark Processing")
                            .master("local[*]")
                            .getOrCreate();
                }
            }
        }
        return instance;
    }

    public static void stop() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }
}
