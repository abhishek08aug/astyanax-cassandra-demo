
package in.blogspot.randomcompiler.astyanax.cassandra.demo;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxClient {

    private static AstyanaxContext<Keyspace> context;
    private static Keyspace keyspace;
    private static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily.newColumnFamily("Standard1",
            StringSerializer.get(), StringSerializer.get());

    static {
        context = new AstyanaxContext.Builder()
                .forCluster("Test Cluster")
                .forKeyspace("keyspacename")
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1)
                                .setSeeds("127.0.0.1:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        keyspace = context.getClient();
        try {
            keyspace.dropKeyspace();
            keyspace.createKeyspace(ImmutableMap
                    .<String, Object> builder()
                    .put("strategy_options",
                            ImmutableMap.<String, Object> builder().put("replication_factor", "1").build())
                    .put("strategy_class", "SimpleStrategy").build());
            keyspace.createColumnFamily(CF_STANDARD1, null);
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ColumnFamily<String, String> CF_USER_INFO = new ColumnFamily<String, String>("Standard1",
                StringSerializer.get(), StringSerializer.get());

        MutationBatch m = keyspace.prepareMutationBatch();

        m.withRow(CF_USER_INFO, "acct1234").putColumn("firstname", "john", null).putColumn("lastname", "smith", null)
                .putColumn("address", "555 Elm St", null).putColumn("age", 30, null);

        try {
            OperationResult<Void> result = m.execute();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        OperationResult<ColumnList<String>> result = null;
        try {
            result = keyspace.prepareQuery(CF_USER_INFO).getKey("acct1234").execute();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        ColumnList<String> columns = result.getResult();
        int age = columns.getColumnByName("age").getIntegerValue();
        System.out.println("Age: " + age);
        String address = columns.getColumnByName("address").getStringValue();

        for (Column<String> c : result.getResult()) {
            System.out.println(c.getName());
            System.out.println(c.getStringValue());
        }
    }

}
