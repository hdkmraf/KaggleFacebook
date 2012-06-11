/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kagglefacebook;

import java.io.File;
import java.io.IOException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;


/**
 *
 * @author rafael
 */
public class Graph {
    
    private String DB_PATH;
    String greeting;
    private GraphDatabaseService graphDb;
    private Index<Node> nameIndex;
    private Index<Node> codeIndex;
    private String NAME_KEY = "name";
    private String DIR;
    private boolean newDB;
    
    
    public Graph(String dir, String db_path, boolean newdb){
        DIR = dir;
        DB_PATH = db_path;
        newDB = newdb;
        if(newDB){
            clearDb();
        }
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        nameIndex = graphDb.index().forNodes(NAME_KEY);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
    }
    
    public void loadFromCSV(String file){
        String [] lines = Helper.readFile(DIR+file).split(System.getProperty("line.separator"));
        System.out.println("Loading "+DIR+file);
        for (int i=1; i<lines.length;i++){
            String [] names = lines[i].split(",");
            createRelationship(names[0],names[1]);
        }
        
    }
    
    
   private enum facebookRelationshipTypes implements RelationshipType{
       relation
   }
    
    
   private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
   
   void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }
    
   // START SNIPPET: shutdownHook
   private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    // END SNIPPET: shutdownHook
    
    public void createRelationship(String from, String to){
         Node fromNode = null;
         Node toNode = null;
         if (from==null || to==null){
            return;
         }
         Transaction tx = graphDb.beginTx();
         try{
            fromNode = getOrCreateAndIndexNode(from);
            toNode = getOrCreateAndIndexNode(to);
            fromNode.createRelationshipTo(toNode, facebookRelationshipTypes.relation);
            tx.success();
         } catch (Exception ex){
             //System.out.println("Problem with graph relationship:"+ex);
             ex.getMessage();
         }
           finally {
             tx.finish();
         }              
    }
    
     private Node getOrCreateAndIndexNode(String name){
        Node node = nameIndex.get(NAME_KEY, name).getSingle();
        if (node == null){        
            node = graphDb.createNode();
            node.setProperty(NAME_KEY, name);        
            nameIndex.add(node, NAME_KEY, name);
        }
        return node;        
    }    
     
}
