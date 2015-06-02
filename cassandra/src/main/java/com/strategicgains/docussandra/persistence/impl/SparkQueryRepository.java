/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra.persistence.impl;

import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Query;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.cassandra.api.java.JavaCassandraSQLContext;

/**
 *
 * @author udeyoje
 */
@Deprecated
public class SparkQueryRepository implements Serializable
{

    public QueryResponseWrapper queryCassandra(Query query)
    {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkTest")
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.native.port", "9042")
                .set("spark.cassandra.connection.rpc.port", "9160");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaCassandraSQLContext sqlContext = new JavaCassandraSQLContext(context);
        sqlContext.sqlContext().setKeyspace("docussandra");

        String sql = "SELECT * FROM docussandra.players_players_table";// + query.getTable() + " WHERE " + query.getWhere();
        JavaSchemaRDD rdd = sqlContext.sql(sql);
        List<Row> rows = rdd.collect();
        List<Document> docs = new ArrayList<>();
        for (Row r : rows)
        {
            //toReturn.add(Utils.)
        }
        QueryResponseWrapper toReturn = new QueryResponseWrapper(docs, Long.MAX_VALUE);
        return toReturn;
    }

    public QueryResponseWrapper queryHadoop(Query query)
    {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkTest")
                .set("spark.executor.memory", "1g");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        String path = "hdfs://localhost:54310/hdocussandra/players/players_table";
        JavaCassandraSQLContext sqlContext = new JavaCassandraSQLContext(context);
        JavaSchemaRDD table = sqlContext.jsonFile(path);
        table.printSchema();
        table.registerTempTable("table1");
        JavaSchemaRDD manning = sqlContext.sql("SELECT * FROM table1 WHERE NAMELAST = 'Manning'");
        List<String> manningRecords = manning.map(new Function<Row, String>()
        {
            public String call(Row row)
            {
                return "Name: " + row.getString(0);
            }
        }).collect();
        return null;
    }
}
