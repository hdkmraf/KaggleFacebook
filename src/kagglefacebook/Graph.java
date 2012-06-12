/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kagglefacebook;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;


/**
 *
 * @author rafael
 */
public class Graph {
    
    private String DB_PATH;
    String greeting;
    private GraphDatabaseService graphDb;
    private Index<Node> nameIndex;
    private String NAME_KEY = "name";
    private String DIR;
    private boolean newDB;
    private int MAX_THREADS = 50;
    
    
    public Graph(String dir, String db_path, boolean newdb){
        DIR = dir;
        DB_PATH = db_path;
        newDB = newdb;
        if(newDB){
            clearDb();
        }       
        startDB();
    }
    
    private void startDB(){
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        nameIndex = graphDb.index().forNodes(NAME_KEY);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
    }
    
    public void loadFromCSV(String file){
        String[] lines = Helper.readFile(DIR+file).split(System.getProperty("line.separator"));
        System.out.println("Loading "+DIR+file);
        int batchSize = (lines.length-1)/MAX_THREADS;
        List<Thread> threads = new ArrayList<Thread>();
        
        for(int i=1; i<lines.length;){
            List<String> batch = new ArrayList<String>();
            for(int j=0; j<batchSize && i<lines.length; j++){
                batch.add(lines[i]);
                i++;
            }            
            threads.add(new Thread(new LoadBatch(batch)));            
            threads.get(threads.size()-1).start();
        }
        
        for (Thread thread: threads){
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Graph.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Finished loading "+DIR+file);
    }
    
   
   public void batchInsertFromCSV(String file){
       graphDb.shutdown();
       
       String[] lines = Helper.readFile(DIR+file).split(System.getProperty("line.separator"));
       System.out.println("batchInserterFromCSV "+DIR+file);
       
       Map<String, String> config = new HashMap<String, String>();
       config.put("use_memory_mapped_buffers","false");
       config.put("neostore.propertystore.db.index.keys.mapped_memory","5M");
       config.put("neostore.propertystore.db.strings.mapped_memory","100M");
       config.put("neostore.propertystore.db.arrays.mapped_memory","107M");
       config.put("neostore.relationshipstore.db.mapped_memory","1000M");
       config.put("neostore.propertystore.db.index.mapped_memory","5M");
       config.put("neostore.propertystore.db.mapped_memory","1000M");
       config.put("dump_configuration","true");
       config.put("cache_type","none");
       config.put("neostore.nodestore.db.mapped_memory","200M");
       BatchInserter inserter = BatchInserters.inserter(DB_PATH, config);       
       
       for (String line:lines){
           Map<String,Object> properties = new HashMap<String, Object>();
           String[] names = line.split(",");
           properties.put(NAME_KEY, names[0]);
           long node1 = inserter.createNode(properties);
           properties.put(NAME_KEY, names[1]);
           long node2 = inserter.createNode(properties);
           inserter.createRelationship(node1, node2, facebookRelationshipTypes.relation, null);       
       }
       inserter.shutdown();      
       startDB();
   }
    
   private enum facebookRelationshipTypes implements RelationshipType{
       relation
   }
     
    
   private void clearDb(){
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
   
   void shutDown(){
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }
    
   // START SNIPPET: shutdownHook
   private static void registerShutdownHook( final GraphDatabaseService graphDb ){
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
          
    
    public class LoadBatch implements Runnable{
        
        private List<String> lines;
        
        public LoadBatch (List<String> lines){
            this.lines = lines;
        }

        @Override
        public void run() {
            String sourceName = "";
            Node sourceNode = null;        
            for (String line : lines){           
                String [] names = line.split(",");
                if(!names[0].equals(sourceName)){
                    sourceName = names[0];
                    sourceNode = getOrCreateAndIndexNode(sourceName);
                }
                createRelationship(sourceName, names[1], sourceNode);
            }
        }
        
         public void createRelationship(String fromName, String toName, Node fromNode) {
            if (fromNode == null)
                fromNode = getOrCreateAndIndexNode(fromName);
            Node toNode = getOrCreateAndIndexNode(toName);
            Transaction tx = graphDb.beginTx();
            try{                        
                fromNode.createRelationshipTo(toNode, facebookRelationshipTypes.relation);
                tx.success();
                //System.out.println(fromNode.getProperty(NAME_KEY) +":"+toNode.getProperty(NAME_KEY));
            } catch (Exception ex){
                 //System.out.println("Problem with graph relationship:"+ex);
                ex.getMessage();
            }
            finally {
                tx.finish();
            }
        }
        
        private Node getOrCreateAndIndexNode(String name){
            Node node = null;
            try{
                IndexHits<Node> hits = nameIndex.get(NAME_KEY, name);
                if(hits.hasNext()){
                    node = hits.next();
                }
                hits.close();
            } catch (Exception e){
                System.out.println(e+":"+name);
            }
            if (node == null){
                Transaction tx = graphDb.beginTx();
                try{
                    node = graphDb.createNode();
                    node.setProperty(NAME_KEY, name);        
                    nameIndex.add(node, NAME_KEY, name);
                    tx.success();
                }
                finally {
                    tx.finish();
                }
            }
            return node;        
        }  
        
    }
    
}
