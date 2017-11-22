package testing;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class SubGraph implements Serializable {
    public static final long serialVersionUID = 0l;
    private ArrayList<Row> vertices;
    private ArrayList<Row> edges;
    private StructType vertexSchema;
    private StructType edgeSchema;

    public SubGraph(StructType vertexSchema, StructType edgeSchema) {
        this.vertexSchema = vertexSchema;
        this.edgeSchema = edgeSchema;
        this.vertices = new ArrayList<Row>();
        this.edges = new ArrayList<Row>();
    }

    public void addVertex(Row vertex) {
        this.vertices.add(vertex);
    }

    public void addEdge(Row edge) {
        this.edges.add(edge);
    }

    public Dataset<Row> getVertices(JavaSparkContext context) {
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);
        Dataset<Row> df = sqlContext.createDataFrame(context.parallelize(this.vertices), this.vertexSchema);
        return df;
    }

    public Dataset<Row> getEdges(JavaSparkContext context) {
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);
        Dataset<Row> df = sqlContext.createDataFrame(context.parallelize(this.edges), this.edgeSchema);
        return df;
    }
}
