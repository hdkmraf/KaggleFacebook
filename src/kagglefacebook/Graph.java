/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kagglefacebook;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
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
    private String NAME_KEY = "name";
    private String DIR;
    private boolean newDB;
    
    private final int MAX_THREADS = Runtime.getRuntime().availableProcessors()/2;
    //private final int MAX_THREADS = 1;
       
    private String NL = System.getProperty("line.separator");
    private Map<String, String> config = new HashMap<String, String>();
    
      
   
    
   
    public Graph(String dir, String db_path, boolean newdb){
        DIR = dir;
        DB_PATH = db_path;
        newDB = newdb;
        if(newDB){
            clearDb();
        }
       config.put("use_memory_mapped_buffers","false");
       config.put("neostore.propertystore.db.index.keys.mapped_memory","2000M");
       config.put("neostore.propertystore.db.strings.mapped_memory","2000M");
       config.put("neostore.propertystore.db.arrays.mapped_memory","2000M");
       config.put("neostore.relationshipstore.db.mapped_memory","2000M");
       config.put("neostore.propertystore.db.index.mapped_memory","2000M");
       config.put("neostore.propertystore.db.mapped_memory","2000M");
       config.put("neostore.nodestore.db.mapped_memory","2000M");
       config.put("dump_configuration","true");
       config.put("cache_type","strong");     
    }
    
    public void startDB(){
        System.out.println("Starting GraphDB with "+MAX_THREADS+" procesors");
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedGraphDatabase( DB_PATH , config);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
    }
    
    public void startReadOnlyDB(){
        System.out.println("Starting GraphDB with "+MAX_THREADS+" procesors");
        // START SNIPPET: startDb
        //graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        graphDb = new EmbeddedReadOnlyGraphDatabase( DB_PATH , config);
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb
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
    
   
   void shutDownDB(){
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
   
   public void cacheFullGraph(){
       long memory = Runtime.getRuntime().totalMemory();
       System.out.println("Loading graph into cache...");
       GlobalGraphOperations operations = GlobalGraphOperations.at(graphDb);
       for (Node n : operations.getAllNodes()){
           IteratorUtil.count(n.getRelationships());
       }
       memory = Runtime.getRuntime().totalMemory() - memory;
       System.out.println("Graph memory = "+memory+ "bytes");
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
       
       String[] lines = Helper.readFile(DIR+file).split(NL);
       System.out.println("batchInserterFromCSV "+DIR+file);
       
       Map<Long, Long> relPerNodeCount = new HashMap<Long, Long>();
       BatchInserter inserter = BatchInserters.inserter(DB_PATH, config);       
       long nodeCount = 0L;
       long relationshipCount = 0L;
       
       for (int i=1; i< lines.length; i++){
           String[] names = lines[i].split(",");
           long node1 = Long.valueOf(names[0]);
           long node2 = Long.valueOf(names[1]);           
           if(!inserter.nodeExists(node1)){
               inserter.createNode(node1,null);
               nodeCount++;
               relPerNodeCount.put(node1, 1L);
           } else{
               relPerNodeCount.put(node1, relPerNodeCount.get(node1)+1);
           }                         
           if(!inserter.nodeExists(node2)){
               inserter.createNode(node2,null);
               nodeCount++;
               relPerNodeCount.put(node2, 1L);
           } else{
               relPerNodeCount.put(node2, relPerNodeCount.get(node2)+1);
           }   
           inserter.createRelationship(node1, node2, facebookRelationshipTypes.relation, null);
           
  
           relationshipCount++;
       }       
       inserter.shutdown();
       SummaryStatistics stats = new SummaryStatistics();
       for(Entry<Long, Long> entry:relPerNodeCount.entrySet()){
           //System.out.println(entry.toString());
           stats.addValue(entry.getValue());
       }
       System.out.println(stats.getSummary());
       System.out.println("Inserted Nodes = "+nodeCount+" Relationships = "+relationshipCount );
   }
    
  
   
   public void splitIntoSets(Integer totalSets, Integer testSetSize, Integer relationshipsPerNode, boolean random){
       System.out.println("splitIntoSets Total sets:"+totalSets+" Set size:"+testSetSize+" Rels per node:"+relationshipsPerNode );
       
       AbstractGraphDatabase abstractGraph = (AbstractGraphDatabase)graphDb;
       Long totalNodes = abstractGraph.getNodeManager().getNumberOfIdsInUse(Node.class);              
       File dbDir = new File(DB_PATH);       
       Set<Long> testNodes = new HashSet<Long>();
       
       for (int i=0; i<totalSets; i++){
           String outFile = DIR + "test_" + i;
           String outDB =DIR+"train_"+i+".db";      
           File outDBDir = new File(outDB);
           try {
               FileUtils.copyRecursively(dbDir, outDBDir);
           } catch (IOException ex) {
               Logger.getLogger(Graph.class.getName()).log(Level.SEVERE, null, ex);
           }
           Graph outGraph = new Graph(DIR, outDB, false);
           outGraph.startDB();
           Helper.writeToFile(outFile, "source_node,destination_nodes"+NL, true);
           for (int j=0; j<testSetSize; j++) {
               String line = "";  
               Node node = null;
               Long fromId = null;
               Set<Relationship> relationships = new HashSet<Relationship>();
               while (node == null || testNodes.contains(fromId) || (random == false && relationships.size()<= relationshipsPerNode)){
                   fromId = 1+((long)(Math.random()*totalNodes));
                   try{
                       node = graphDb.getNodeById(fromId);                                                 
                       relationships = Sets.newHashSet(node.getRelationships(facebookRelationshipTypes.relation, Direction.OUTGOING));                                       
                   } catch (Exception e){
                       node = null;
                       relationships = null;
                       continue;
                   }
                   //System.out.println(fromId);
               }
               
               testNodes.add(fromId);
               //Iterable<Relationship> relationships = node.getRelationships(facebookRelationshipTypes.relation, Direction.OUTGOING);               
               line += fromId+",";
               int k = 0;
               for (Relationship relationship: relationships){
                   if(k>= relationshipsPerNode)
                       break;
                   Long toId = relationship.getOtherNode(node).getId();
                   line += toId+" ";
                   Transaction outTx = outGraph.graphDb.beginTx();
                   try{                        
                       outGraph.graphDb.getRelationshipById(relationship.getId()).delete();
                       outTx.success();
                   } catch (Exception ex){
                       ex.getMessage();
                   } finally {
                       outTx.finish();
                   }                   
                   k++;
               }
               line = line.substring(0, line.length()-1)+NL;
               Helper.writeToFile(outFile, line, false);
           }           
           outGraph.shutDownDB();
           System.out.println("Finished "+outFile);
       }                   
   }
   
     
   public void validateResult(String testFile, String resultFile){
       String[] resultLines = Helper.readFile(DIR+resultFile).split(NL);
       String[] testLines = Helper.readFile(DIR+testFile).split(NL);              
       SummaryStatistics totalAccuracy = new SummaryStatistics();
       
       for(int i=1; i<testLines.length && i<resultLines.length; i++){
           String[] rl = resultLines[i].split(",");
           String[] tl = testLines[i].split(",");
           Double nodeAccuracy = 1.0;
           if(rl[0] == null ? tl[0] == null : rl[0].equals(tl[0])){
               HashSet<String> resultSet = new HashSet<String>();
               if(rl.length>1)
                   resultSet.addAll(Arrays.asList(rl[1].split(" ")));
               
               HashSet<String> testSet = new HashSet<String>();                              
               if(tl.length>1)
                   testSet.addAll(Arrays.asList(tl[1].split(" ")));
               
               HashSet<String> compareSet = new HashSet<String>();
               Double total = (double)(testSet.size()+resultSet.size());               
               if(total>0){
                   compareSet.addAll(testSet);               
                   compareSet.addAll(resultSet);
                   Double correct = total - compareSet.size();                        
                   nodeAccuracy = correct / compareSet.size();
               }
           }    
           System.out.println(i+":"+tl[0]+":"+nodeAccuracy);
           totalAccuracy.addValue(nodeAccuracy);                      
       }
       System.out.println("Accuracy:"+totalAccuracy.getSummary());
   }
           
   
   public void makePredictions(String file, String resultFile){   
       String[] lines = Helper.readFile(DIR+file).split(NL);
       System.out.println("makePredictions "+DIR+file);
       int batchSize = (lines.length-1)/MAX_THREADS;
       List<Thread> threads = new ArrayList<Thread>(); 
       List<String> batchFiles = new ArrayList<String>(); 
       int batchCount=0;
       for(int i=1; i<lines.length;){
           List<Long> batch = new ArrayList<Long>();
           for(int j=0; j<batchSize && i<lines.length; j++){
               batch.add(Long.valueOf(lines[i].split(",")[0]));
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
       
       
       resultFile = DIR+resultFile;
       Helper.writeToFile(resultFile, "source_node,destination_nodes"+NL, true);
       for (String batchFile:batchFiles){
           lines = Helper.readFile(batchFile).split(NL);
           for (String line: lines){
               Helper.writeToFile(resultFile, line+NL, false);
           }
       }
       
       System.out.println("Finished predicting "+resultFile); 
   }
     
   
   public void getCorrectWeights(String testFile){
       Map<Long,Set<Long>> rels = new HashMap<Long,Set<Long>>();
       String[] lines = Helper.readFile(DIR+testFile).split(NL);
       for(int i=1; i<lines.length; i++){
           Set<Long> targets = new HashSet<Long>();
           String[] pairs = lines[i].split(",");  
           if(pairs.length>1){
               for(String target: pairs[1].split(" ")){
                   targets.add(Long.valueOf(target));
               }
           }
           rels.put(Long.valueOf(pairs[0]), targets);           
       }              
       new PredictBatch(new ArrayList<Long>(), "test_batch").correctWeights(rels);
       
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
            if (node == null){
                Transaction tx = graphDb.beginTx();
                try{
                    node = graphDb.createNode();
                    node.setProperty(NAME_KEY, name);        
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
        private final Double MIN_WEIGHT = 0.08;
        private final Integer MAX_DEPTH = 2;
        private final Integer EXTRA_DEPTH = 0;
        private final Integer MAX_ITERATIONS = 1000;
        private final Long TIME_LIMIT = 60000L;        
        
        private final TraversalDescription PREDICTION_TRAVERSAL = 
            Traversal.description()
            .breadthFirst()
            .uniqueness(Uniqueness.NODE_GLOBAL)
            .evaluator(Evaluators.excludeStartPosition())                        
            .evaluator(Evaluators.includingDepths(2, MAX_DEPTH));     
        
        
        public PredictBatch(List<Long> nodes, String outfile){
            this.nodes = nodes;            
            this.outFile = outfile;
            Helper.writeToFile(outFile, "", true);
        }

        @Override
        public void run() {
            for(Long nodeId: nodes){
                Node node = graphDb.getNodeById(nodeId);
                int totalNodes = 10;
                Set<NodeStats> bestNodes = new HashSet<NodeStats>();
                bestNodes.addAll(predictRelatedNodes(node, bestNodes, totalNodes,  Direction.OUTGOING));
                if (bestNodes.size()<totalNodes){
                    bestNodes.addAll(predictRelatedNodes(node, bestNodes, totalNodes - bestNodes.size(), Direction.INCOMING));          
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
        
        public void correctWeights(Map<Long,Set<Long>> rels){
            SummaryStatistics stats = new SummaryStatistics();
            Long time = System.currentTimeMillis();
            for(Long nodeId: rels.keySet()){                
                Node source = graphDb.getNodeById(nodeId);
                for(Long t: rels.get(nodeId)){
                    Long millis = System.currentTimeMillis();
                    Node target = graphDb.getNodeById(t);                    
                    Double weight = getRelationshipWeightSimRankCommonPaths(source, target, 5, 0);
                    stats.addValue(weight);
                    millis = System.currentTimeMillis() -millis;
                    System.out.println(nodeId+" : "+t+" = "+weight+" - "+stats.getMean()+" mS:"+millis);
                }
            }
            time = System.currentTimeMillis() - time;
            System.out.print(stats.getSummary());
            System.out.println("Total seconds:" + time/60);
        }
        
        private Set<NodeStats> predictRelatedNodes(Node origin, Set<NodeStats> bestNodes, Integer totalNodes, Direction direction){
            Set<NodeStats> predictedNodes = new HashSet<NodeStats>();        
            
            int iterationsWithoutImprovement = 0;
            Double prevMeanWeight = 0.0;
            int depth = 0;                   
            long elapsedTime = 0;
            long startTime = System.currentTimeMillis();            
            Traverser traverser = PREDICTION_TRAVERSAL.relationships(facebookRelationshipTypes.relation, direction).traverse(origin);
            for(Path path: traverser){                             
                depth = path.length();                                           
                int skip = 0;
                for(Node endNode: path.nodes()){                        
                    if (skip<2){
                        skip++;
                        continue;
                    }
                    String print = "";
                    if (iterationsWithoutImprovement > MAX_ITERATIONS)                    
                        break; 
                    if(elapsedTime > TIME_LIMIT){
                        if(predictedNodes.size()>=totalNodes)
                            break;
                    } 
                    Double weight = 0.0;                
                    Boolean nodeIn = false;
                    for(NodeStats n : bestNodes){
                        if(n.NODE_ID.equals(endNode.getId())){
                            nodeIn = true;
                            break;
                        }
                    }
                
                    if(!nodeIn)
                        //weight = getRelationshipWeightAdamic(origin,endNode, depth+EXTRA_DEPTH, direction);
                        //weight = getRelationshipWeightKatz(origin,endNode, depth+EXTRA_DEPTH, direction);
                        weight = getRelationshipWeightSimRankCommonPaths(origin, endNode, 5,0);
                    if (weight>MIN_WEIGHT)
                        predictedNodes.add(new NodeStats(endNode.getId(), weight));
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
                    //System.out.println(print);
                }
            }
            //System.out.println();
            return predictedNodes;
        }
   
   
        private Double getRelationshipWeightA(Node from, Node to, int maxDepth, Direction direction){
            
            PathFinder<Path> singleFinder = GraphAlgoFactory.allSimplePaths(Traversal.expanderForAllTypes(Direction.BOTH),1);
            PathFinder<Path> outFinder = GraphAlgoFactory.allSimplePaths(Traversal.expanderForAllTypes(direction),maxDepth);
            
            Iterable<Path> paths = outFinder.findAllPaths(from, to);
            
            SummaryStatistics stats = new SummaryStatistics();
            Set<Node> previousNodes = new HashSet<Node>();
            
            Double pathCount = 0.0;
            Double avgPathLength = 0.0;               
            for(Path path:paths){
                SummaryStatistics pathWeight = new SummaryStatistics();
                Double pathLength = Double.valueOf(path.length());
                
                for (Node n1 : path.nodes()){
                    if(!n1.equals(from) || !n1.equals(to)){
                        for (Node n2 : previousNodes){
                            if(singleFinder.findSinglePath(n1, n2) != null){
                                stats.addValue(1);
                            }
                        }
                        previousNodes.add(n1);
                    }
                }

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
        
        private Double getRelationshipWeightAdamic(Node from, Node to, int maxDepth, Direction direction){
            Double relationshipWeight = 0.0;
            SummaryStatistics relWeight = new SummaryStatistics();
            
            Iterable<Relationship> fromRelationships = from.getRelationships(direction);
            Iterable<Relationship> toRelationships = to.getRelationships(Direction.BOTH);
            //Set<Node> fromNodes = new HashSet<Node>();
            Map<Node,Double> fromNodes = new HashMap<Node,Double>();
            
            for(Relationship fr : fromRelationships){
                if(fr.getStartNode().equals(from))
                    fromNodes.put(fr.getOtherNode(from),OUTGOING_WEIGHT);
                else
                    fromNodes.put(fr.getOtherNode(from),INCOMING_WEIGHT);
            }
            Double toNeighbourCount = 0.0;
            for(Relationship tr : toRelationships){
                Node n = tr.getOtherNode(to);                
                if(fromNodes.containsKey(n)){
                    if(tr.getEndNode().equals(to))
                        relWeight.addValue(((OUTGOING_WEIGHT)+fromNodes.get(n))/2.0);
                    else
                        relWeight.addValue(((INCOMING_WEIGHT)+fromNodes.get(n))/2.0);
                }
                toNeighbourCount++;
            }  
            if (toNeighbourCount>1)
                relationshipWeight = relWeight.getSum()/Math.log(toNeighbourCount);
                //relationshipWeight = relWeight.getSum()/toNeighbourCount;
            else
                relationshipWeight = relWeight.getSum();
            
            //System.out.println(relationshipWeight+" "+relWeight.getSummary());            
            return relationshipWeight;
        }
        
        private Double getRelationshipWeightKatz(Node from, Node to, int maxDepth, Direction direction){
            Double relationshipWeight = 0.0;
            SummaryStatistics relWeight = new SummaryStatistics();
            
            PathFinder<Path> outFinder = GraphAlgoFactory.allSimplePaths(Traversal.expanderForAllTypes(direction),maxDepth);            
            Iterable<Path> paths = outFinder.findAllPaths(from, to);
            
            Double BETA = 0.6;
            Map<Integer,Integer> pathCount = new HashMap<Integer,Integer>();
            for(Path path: paths){
                Integer lenght = path.length();
                if(!pathCount.containsKey(lenght)){
                    pathCount.put(lenght, 1);
                } else{
                    pathCount.put(lenght, pathCount.get(lenght)+1);
                }
            }
            
            for(Entry<Integer,Integer> entry:pathCount.entrySet()){
                relWeight.addValue(Math.pow(BETA, entry.getKey())*entry.getValue());                
            }                                                
            relationshipWeight = relWeight.getSum();
            //System.out.println(relWeight.getSummary());
            return relationshipWeight;
        }
        
        private Double getRelationshipWeightSimRank(Node x, Set<Relationship> xRelationships, Node y, Set<Relationship> yRelationships , int maxDepth, int depth){            
            if (x.equals(y))
                return 1.0; 
            if (depth>=maxDepth)
                return 0.0;            
            
            Double decay = 0.1;
            Double threshold = 0.0001;
            Double relationshipWeight = 0.0;                                       
            
            Double noScoreWeight = 0.0;
            if(xRelationships.size()>0 && yRelationships.size()>0)
                noScoreWeight = decay/(xRelationships.size()*yRelationships.size());
            else
                return 0.0;           
            
            if(noScoreWeight*Math.pow(decay,depth) < threshold){
                //System.out.println(depth+" "+noScoreWeight);
                return noScoreWeight;
            }
                                                            
            SummaryStatistics relWeight = new SummaryStatistics();                          
            boolean oppositeFound = false;
            for(Relationship xr : xRelationships){
                Node a = xr.getStartNode();
                if(a.equals(y)){
                    if(!oppositeFound){                      
                        oppositeFound = true;
                        if (depth>0)
                            relWeight.addValue(1.0); 
                    }
                    continue;
                }
                Set<Relationship> aRelationships = Sets.newHashSet(a.getRelationships(Direction.INCOMING));              
                for(Relationship yr : yRelationships){                                    
                    Node b = yr.getStartNode();
                    if(b.equals(x)){
                        if(!oppositeFound){
                            oppositeFound = true;
                            if (depth>0)
                                relWeight.addValue(1.0);                            
                        }
                        continue;
                    }                        
                    else{
                        //System.out.println(a.getId()+" => "+b.getId());
                        Set<Relationship> bRelationships = Sets.newHashSet(b.getRelationships(Direction.INCOMING));
                        if (aRelationships.size()<1)
                            continue;
                        Double score = getRelationshipWeightSimRank(a, aRelationships, b, bRelationships, maxDepth, depth+1);
                        relWeight.addValue(score);                    
                    }
                }
            }            
            
            
            relationshipWeight = relWeight.getSum()*noScoreWeight;       
            //System.out.println(depth+" "+relationshipWeight);
            return relationshipWeight;
        }
        
         private Double getRelationshipWeightSimRankCommonPaths(Node x, Node y, int maxDepth, int depth){
            if (depth>=maxDepth)
                return 0.0;            
            
            Double decay = 0.1;
            Double threshold = 0.01;
            Double relationshipWeight = 0.0;               
                       
            PathFinder<Path> outFinder = GraphAlgoFactory.pathsWithLength(Traversal.expanderForAllTypes(Direction.OUTGOING),2);            
            Iterable<Path> paths = outFinder.findAllPaths(x, y);
            Set<Node> commonNodes = new HashSet<Node>();                       
            
            SummaryStatistics relWeight = new SummaryStatistics();                                    
            for(Path p: paths){               
                for(Node n: p.nodes()){                                    
                    commonNodes.add(n);
                }
            }
                                               
            if (commonNodes.size()<1)
                return 0.0;   
            
            commonNodes.remove(x);
            commonNodes.remove(y);            
            
            Set<Relationship> xRelationships = Sets.newHashSet(x.getRelationships(Direction.OUTGOING));
            Set<Relationship> yRelationships = Sets.newHashSet(y.getRelationships(Direction.INCOMING));            
            Double noScoreWeight = 1.0/(xRelationships.size()*xRelationships.size());
            Double commonNodesSquare = Math.pow(commonNodes.size(),2);
            if((noScoreWeight*Math.pow(decay,depth)*commonNodesSquare) < threshold){
                //System.out.println(depth+" "+noScoreWeight);
                return noScoreWeight*commonNodesSquare;
            }

            //Set<Node> commonNodesCopy = new HashSet<Node>();
            //commonNodesCopy.addAll(commonNodes);
            for (Node a: commonNodes){  
                //commonNodesCopy.remove(a);
                for (Node b: commonNodes){
                    Double score = getRelationshipWeightSimRankCommonPaths(a, b, maxDepth, depth+1);                 
                    relWeight.addValue(score);
                }
            }                                                   
            
            relationshipWeight = relWeight.getSum()*noScoreWeight*decay;       
            //System.out.println(depth+" "+relationshipWeight);
            return relationshipWeight;
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
            else{                            
                if(this.NODE_ID > n.NODE_ID)
                    return 1;
                else if(this.NODE_ID < n.NODE_ID)
                    return -1;
                else
                    return 0;            
            }
        }
        
    }
}
