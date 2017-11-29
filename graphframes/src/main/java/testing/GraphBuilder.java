package testing;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GraphBuilder {
    private static class GlobalId implements Serializable {
        public static final long serialVersionUID = 0l;
        private static GlobalId instance = null;
        private long current = 0;
        protected GlobalId() {
           // Exists only to defeat instantiation.
        }
        public static GlobalId getInstance() {
           if(instance == null) {
              instance = new GlobalId();
           }
           return instance;
        }

        public long getNext() {
            return ++current;
        }
    }

    private static class LoadedUserIds implements Serializable {
        public static final long serialVersionUID = 0l;
        private static LoadedUserIds instance = null;
        private Set<Long> loadedIds;

        protected LoadedUserIds() {
            this.loadedIds = new HashSet<>();
        }

        public static LoadedUserIds getInstance() {
            if (instance == null) {
                instance = new LoadedUserIds();
            }

            return instance;
        }

        public void add(Long id) {
            loadedIds.add(id);
        }

        public boolean has(Long id) {
            return loadedIds.contains(id);
        }
    }

    private static class LoadedCallerIds implements Serializable {
        public static final long serialVersionUID = 0l;
        private static LoadedCallerIds instance = null;
        private Set<Long> loadedIds;

        protected LoadedCallerIds() {
            this.loadedIds = new HashSet<>();
        }

        public static LoadedCallerIds getInstance() {
            if (instance == null) {
                instance = new LoadedCallerIds();
            }

            return instance;
        }

        public void add(Long id) {
            loadedIds.add(id);
        }

        public boolean has(Long id) {
            return loadedIds.contains(id);
        }
    }
    private static StructType buildVertexSchema() {
        List<StructField> vertexFields = new ArrayList<StructField>();
        vertexFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertexFields.add(DataTypes.createStructField("typeId", DataTypes.LongType, true));
        vertexFields.add(DataTypes.createStructField("startTime", DataTypes.TimestampType, true));
        vertexFields.add(DataTypes.createStructField("duration", DataTypes.FloatType, true));
        vertexFields.add(DataTypes.createStructField("numero", DataTypes.StringType, true));
        vertexFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        vertexFields.add(DataTypes.createStructField("type", DataTypes.StringType, false));
        return DataTypes.createStructType(vertexFields);
    }

    private static StructType buildEdgeSchema() {
        List<StructField> edgeFields = new ArrayList<StructField>();
        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("type", DataTypes.StringType, false));
        return DataTypes.createStructType(edgeFields);
    }

    private static Long parseLong(Object o) {
        return Long.valueOf(((String) o).trim());
    }

    private static Timestamp parseTimestamp(Object string) {
        String str = (String) string;
        return Timestamp.valueOf(str.trim().substring(1, 20));
    }

    private static Float parseFloat(Object string) {
        return Float.valueOf(((String) string).replaceAll("\"", ""));
    }

    public static SubGraph buildTelefonos(List<Row> telefonos) {
        SubGraph subGraph = new SubGraph(buildVertexSchema(), buildEdgeSchema());
        telefonos.forEach((row) -> {
            if (LoadedUserIds.getInstance().has(parseLong(row.get(1)))) {
                Row user = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(1)), null, null, null, null, "usuario");
                subGraph.addVertex(user);
                LoadedUserIds.getInstance().add(parseLong(row.get(1)));
            }
            Row telephone = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(0)), null, null, row.get(2), null, "telefono");
            Row city = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(3), "ciudad");
            Row country = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(4), "pais");
            Row provider = RowFactory.create(GlobalId.getInstance().getNext(), null, null, null, null, row.get(5), "proveedor");
            Row userHasTelephone = RowFactory.create(parseLong(row.get(1)), parseLong(row.get(0)), "tiene_telefono");
            Row cityBelongsToCountry = RowFactory.create(city.get(0), country.get(0), "queda_en");
            Row userLivesIn = RowFactory.create(parseLong(row.get(1)), city.get(0), "vive_en");
            subGraph.addVertex(telephone);
            subGraph.addEdge(userHasTelephone);
            subGraph.addVertex(city);
            subGraph.addVertex(country);
            subGraph.addVertex(provider);
            subGraph.addEdge(cityBelongsToCountry);
            subGraph.addEdge(userLivesIn);
        });
        return subGraph;
    }

    public static SubGraph buildLlamadas(List<Row> llamadas) {
        SubGraph subGraph = new SubGraph(buildVertexSchema(), buildEdgeSchema());
        llamadas.forEach((row) -> {
            Row call = RowFactory.create(GlobalId.getInstance().getNext(), parseLong(row.get(0)), parseTimestamp(row.get(1)), parseFloat(row.get(2)), null, null, "llamada");
            subGraph.addVertex(call);
            if (!LoadedCallerIds.getInstance().has(parseLong(row.get(3)))) {
                Row userStartedCall = RowFactory.create(parseLong(row.get(3)), parseLong(row.get(0)), "creo");
                LoadedCallerIds.getInstance().add(parseLong(row.get(3)));
                subGraph.addEdge(userStartedCall);
            }
            Row userWasInCall = RowFactory.create(parseLong(row.get(4)), parseLong(row.get(0)), "recibio");
            subGraph.addEdge(userWasInCall);
        });
        return subGraph;
    }
}
