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
        if(true){
            Graph graph = new Graph(dir, dir+"graph.db", newGraph);                
            if(newGraph){
                //graph.startDB();
                //graph.shutDownDB();
                graph.batchInsertFromCSV("train.csv");
                
            }
            graph.startReadOnlyDB();
            //graph.cacheFullGraph();
            //graph.splitIntoSets(1, 100, 10, false);
            //graph.getCorrectWeights("test_full");
            //graph.getCorrectWeights("test_random");
            //graph.getCorrectWeights("train.csv");
            //graph.getCorrectWeights("test_100");
            //graph.makePredictions("test.csv", "submission_result.csv", false);
            graph.makePredictions("selection_PR", "submission_result.csv", true);
            graph.shutDownDB();
        }
        
        
        if(false){
            String selector = "small";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector, false);      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
        
        if(false){
            String selector = "full";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector, false);      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
        
        if(false){
            String selector = "random";        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector, false);      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
    }
}
