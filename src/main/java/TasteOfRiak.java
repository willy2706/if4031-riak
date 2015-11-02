import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;

public class TasteOfRiak {

    // This will create a client object that we can use to interact with Riak
    private static RiakCluster setUpCluster() throws UnknownHostException {
        // This example will use only one node listening on localhost:10017
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("167.205.33.191")
                .withRemotePort(8087)
                .build();

        // This cluster object takes our one node as an argument
        RiakCluster cluster = new RiakCluster.Builder(node)
                .build();

        // The cluster must be started to work, otherwise you will see errors
        cluster.start();

        return cluster;
    }

    public static void main( String[] args ) {
        try {
            // First, we'll create a basic object storing a movie quote
            RiakObject quoteObject = new RiakObject()
                    // We tell Riak that we're storing plaintext, not JSON, HTML, etc.
                    .setContentType("text/plain")
                            // Objects are ultimately stored as binaries
                    .setValue(BinaryValue.create("You're dangerous, Maverick - testing failure?\n"));
            System.out.println("Basic object created");

            // In the new Java client, instead of buckets you interact with Namespace
            // objects, which consist of a bucket AND a bucket type; if you don't
            // supply a bucket type, "default" is used; the Namespace below will set
            // only a bucket, without supplying a bucket type
            Namespace quotesBucket = new Namespace("willy65");

            // With our Namespace object in hand, we can create a Location object,
            // which allows us to pass in a key as well
            Location quoteObjectLocation = new Location(quotesBucket, "mykey");
            System.out.println("Location object created for quote object");

            // And now we can use our setUpCluster() function to create a cluster
            // object which we can then use to create a client object and then
            // execute our storage operation
            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);
            System.out.println("Client object successfully created");

            StoreValue storeOp = new StoreValue.Builder(quoteObject)
                    .withLocation(quoteObjectLocation)
                    .withOption(StoreValue.Option.W, new Quorum(3))
                    .build();
            StoreValue.Response response = client.execute(storeOp);

            // Now that we're all finished, we should shut our cluster object down
            cluster.shutdown();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}