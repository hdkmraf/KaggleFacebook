/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kagglefacebook;

/**
 *
 * @author rafael
 */
public class KaggleFacebook {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String dir = "/home/rafael/kaggle_facebook_dump/";                     
        // newGraph = true will overwrite the neo4j graph and googlechart.js
        boolean newGraph = false;        
        if(false){
            Graph graph = new Graph(dir, dir+"graph.db", newGraph);                
            if(newGraph){
                //graph.startDB();
                //graph.shutDownDB();
                graph.batchInsertFromCSV("train.csv");
                
            }
            graph.startReadOnlyDB();
            //graph.cacheFullGraph();
            //graph.splitIntoSets(1, 1000, 10, true);
            graph.getCorrectWeights("test_full");
            //graph.getCorrectWeights("test_random");
            //graph.getCorrectWeights("train.csv");
            //graph.makePredictions("test.csv", "submission_result.csv");
            graph.shutDownDB();
        }
        
        
        if(false){
            String selector = "0";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector );      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
        
        if(true){
            String selector = "full";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector );      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
        
        if(false){
            String selector = "random";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector );      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
    }
}
