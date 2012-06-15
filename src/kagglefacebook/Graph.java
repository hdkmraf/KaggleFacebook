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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import scala.actors.threadpool.Arrays;


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
    
    private final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    //private final int MAX_THREADS = 1;
       
    private String NL = System.getProperty("line.separator");
    private Random random = new Random();
    private Map<String, String> config = new HashMap<String, String>();
    
      
   
    
    
    public Graph(String dir, String db_path, boolean newdb){
        DIR = dir;
        DB_PATH = db_path;
        newDB = newdb;
        if(newDB){
            clearDb();
        }
       config.put("use_memory_mapped_buffers","false");
       config.put("neostore.propertystore.db.index.keys.mapped_memory","5M");
       config.put("neostore.propertystore.db.strings.mapped_memory","100M");
       config.put("neostore.propertystore.db.arrays.mapped_memory","107M");
       config.put("neostore.relationshipstore.db.mapped_memory","2000M");
       config.put("neostore.propertystore.db.index.mapped_memory","5M");
       config.put("neostore.propertystore.db.mapped_memory","1000M");
       config.put("dump_configuration","true");
       config.put("cache_type","none");
       config.put("neostore.nodestore.db.mapped_memory","500M");
        startDB();
    }
    
    private void startDB(){
        System.out.println("Starting GraphDB with "+MAX_THREADS+" procesors");
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedGraphDatabase( DB_PATH , config);
        nameIndex = graphDb.index().forNodes(NAME_KEY);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
    }
    
    public void loadFromCSV(String file){
        String[] lines = Helper.readFile(DIR+file).split(NL);
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
       
       String[] lines = Helper.readFile(DIR+file).split(NL);
       System.out.println("batchInserterFromCSV "+DIR+file);
       
       BatchInserter inserter = BatchInserters.inserter(DB_PATH, config);       
       
       for (int i=1; i< lines.length; i++){
           Map<String,Object> properties = new HashMap<String, Object>();
           String[] names = lines[i].split(",");
           long node1 = Long.valueOf(names[0]);
           long node2 = Long.valueOf(names[1]);           
           if(!inserter.nodeExists(node1)){
               properties.put(NAME_KEY, node1);
               inserter.createNode(node1,properties);
           }                          
           if(!inserter.nodeExists(node2)){
               properties.put(NAME_KEY, node2);
               inserter.createNode(node2,properties);
           } 
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
   
   public void makePredictions(String file){     
       String[] lines = Helper.readFile(DIR+file).split(NL);
       System.out.println("makePredictions "+DIR+file);
       int batchSize = (lines.length-1)/MAX_THREADS;
       List<Thread> threads = new ArrayList<Thread>(); 
       List<String> batchFiles = new ArrayList<String>(); 
       int batchCount=0;
       for(int i=1; i<lines.length;){
           List<Long> batch = new ArrayList<Long>();
           for(int j=0; j<batchSize && i<lines.length; j++){
               batch.add(Long.valueOf(lines[i]));
               i++;           
           }      
           String batchFile = DIR+"batch_"+batchCount;
           batchFiles.add(batchFile);
           batchCount++;
           threads.add(new Thread(new PredictBatch(batch, batchFile)));            
           threads.get(threads.size()-1).start();           
       }
       
       lines = null;
        
       for (Thread thread: threads){
           try {
               thread.join();
           } catch (InterruptedException ex) {
               Logger.getLogger(Graph.class.getName()).log(Level.SEVERE, null, ex);
           }
       }
       
       
       String resultFile = DIR+"result.csv";
       Helper.writeToFile(resultFile, "source_node,destination_nodes"+NL, false);
       for (String batchFile:batchFiles){
           lines = Helper.readFile(batchFile).split(NL);
           for (String line: lines){
               Helper.writeToFile(resultFile, line+NL, false);
           }
       }
       
       System.out.println("Finished predicting "+resultFile);             
   }
        
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
     
    public class PredictBatch implements Runnable{       
        private List<Long> nodes;
        private String outFile;
        
        private final Double OUTGOING_WEIGHT = 1.0;
        private final Double INCOMING_WEIGHT = 0.1;
        private final Integer MAX_DEPTH = 2;
        private final Integer EXTRA_DEPTH = 0;
        private final Integer MAX_ITERATIONS = 1000;
        private final Long TIME_LIMIT = 60000L;
        
        private final TraversalDescription PREDICTION_TRAVERSAL = 
            Traversal.description()
            .breadthFirst()
            .uniqueness(Uniqueness.NODE_GLOBAL);      
        
        
        public PredictBatch(List<Long> nodes, String outfile){
            this.nodes = nodes;            
            this.outFile = outfile;
        }

        @Override
        public void run() {
            for(Long nodeId: nodes){
                Node node = graphDb.getNodeById(nodeId);
                int totalNodes = 10;
                List<NodeStats> bestNodes = new ArrayList<NodeStats>();
                bestNodes = predictRelatedNodes(node, bestNodes, totalNodes,  Direction.OUTGOING);
                if (bestNodes.size()<totalNodes){
                    bestNodes.addAll(predictRelatedNodes(node, bestNodes, totalNodes - bestNodes.size(), Direction.BOTH));          
                }
                String nodesString = "";
                for(NodeStats n:bestNodes){
                    nodesString = nodesString + n.NODE_ID + " ";
                }
                if(nodesString.length()>0)
                    nodesString = nodesString.substring(0, nodesString.length()-1);
                Helper.writeToFile(outFile, nodeId+","+nodesString+NL, false);
                //System.out.println(nodeId+","+nodesString);
            }
        }
        
        private List<NodeStats> predictRelatedNodes(Node origin, List<NodeStats> bestNodes, Integer totalNodes, Direction direction){
            List<NodeStats> predictedNodes = new ArrayList<NodeStats>();        
            
            int iterationsWithoutImprovement = 0;
            Double prevMeanWeight = 0.0;
            int depth = 0;                   
            long elapsedTime = 0;
            long startTime = System.currentTimeMillis();
            Traverser traverser = PREDICTION_TRAVERSAL.relationships(facebookRelationshipTypes.relation, direction).traverse(origin);
            for(Path position: traverser){              
                String print = "";
                depth = position.length();
                if(depth<2)
                    continue;
                if(depth > MAX_DEPTH)
                    break;
                if(elapsedTime > TIME_LIMIT){
                    if(predictedNodes.size()>=totalNodes)
                        break;
                }
                if (iterationsWithoutImprovement > MAX_ITERATIONS)                    
                    break;
                Node endNode = position.endNode();
                Double weight = 0.0;
                
                Boolean nodeIn = false;
                for(NodeStats n : bestNodes){
                    if(n.NODE_ID.equals(endNode.getId())){
                        nodeIn = true;
                        break;
                    }
                }
                
                if(!nodeIn)
                    weight = getRelationshipWeight(origin,endNode, depth+EXTRA_DEPTH, direction);
                if (weight>0)
                    predictedNodes.add(new NodeStats(position.endNode().getId(), weight));
                if (predictedNodes.size()>totalNodes){
                    Object[] nodesArray = predictedNodes.toArray();
                    Arrays.sort(nodesArray);
                    predictedNodes.clear();
                    predictedNodes.addAll(Arrays.asList(nodesArray).subList(1, nodesArray.length));         
                    SummaryStatistics stats = new SummaryStatistics();
                                        
                    for(NodeStats n:  predictedNodes){            
                        stats.addValue(n.WEIGHT);                    
                       print += n.WEIGHT+" ";
                    }                                        
                    
                    Double newMeanWeight =  stats.getMean();
                    if(newMeanWeight > prevMeanWeight){
                        iterationsWithoutImprovement = 0;
                    } else {
                        iterationsWithoutImprovement++;
                    }
                    prevMeanWeight = newMeanWeight;           
                }           
                elapsedTime = System.currentTimeMillis()- startTime;
                
                print = depth+":"+iterationsWithoutImprovement+":"+elapsedTime+":"+print;
                System.out.println(print);
            }
            System.out.println();
            return predictedNodes;
        }
   
   
        private Double getRelationshipWeight(Node from, Node to, int maxDepth, Direction direction){
            PathFinder<Path> outFinder = GraphAlgoFactory.allSimplePaths(Traversal.expanderForAllTypes(direction),maxDepth);       
            Iterable<Path> paths = outFinder.findAllPaths(from, to);
            
            SummaryStatistics stats = new SummaryStatistics();
            
            Double pathCount = 0.0;
            Double avgPathLength = 0.0;            
            for(Path path:paths){ 
                SummaryStatistics pathWeight = new SummaryStatistics();
                Double pathLength = Double.valueOf(path.length());
                if(pathLength>1){                    
                    if(direction.equals(Direction.OUTGOING)){
                        for(int i=0; i<pathLength; i++){
                            pathWeight.addValue(OUTGOING_WEIGHT*Math.pow(2,-1*i));
                        }
                    } else if(direction.equals(Direction.INCOMING)){
                        for(int i=0; i<pathLength; i++){
                            pathWeight.addValue(INCOMING_WEIGHT*Math.pow(2,-1*i));
                        }
                    } else {
                        Node node1 = from;
                        Node node2;
                        int i=0;
                        
                        for (Relationship relationship : path.relationships()){
                            node2 = relationship.getOtherNode(node1);
                            if (relationship.getStartNode().equals(node1)){
                                pathWeight.addValue(OUTGOING_WEIGHT*Math.pow(2,-1*i));                              
                            } else {
                                pathWeight.addValue(INCOMING_WEIGHT*Math.pow(2,-1*i));
                            }
                            node1 = node2;
                            i++;
                        }
                    }
                    avgPathLength += pathLength;
                    pathCount++;
                } 
                else
                    return 0.0;
                stats.addValue(pathWeight.getMean());
            }           
            avgPathLength /= pathCount;
                        
            
            //Double nodeWeight = stats.getMean()*stats.getN();
            Double nodeWeight = stats.getSum();
            //Double std = stats.getStandardDeviation();
            //if(std>0)
              //  nodeWeight /= std;
            
            //System.out.println(stats.toString());
            //System.out.println(nodeWeight+":"+avgPathLength+":"+pathCount);
            return nodeWeight;
        }       
        
    }
    
    
    public class NodeStats implements Comparable{        
        public Long NODE_ID;
        public Double WEIGHT;
        
        
        public NodeStats(Long nodeid, Double weight){
            this.NODE_ID = nodeid;
            this.WEIGHT = weight;
        }        

        @Override
        public int compareTo(Object t) {
            NodeStats n = (NodeStats) t;
            if (this.WEIGHT > n.WEIGHT)
                return 1;
            else if (this.WEIGHT < n.WEIGHT)
                return -1;
            else
                return 0;            
        }
        
    }
}
