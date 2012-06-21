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
            //graph.splitIntoSets(1, 1000, 10, false);
            //graph.getCorrectWeights("test_0");
            graph.makePredictions("test.csv", "submission_result.csv");
            graph.shutDownDB();
        }
        
        
        if(false){
            int selector = 0;        
            Graph trainGraph = new Graph(dir, dir+"train_"+selector+".db", false);
            trainGraph.startReadOnlyDB();
            //trainGraph.cacheFullGraph();
            trainGraph.makePredictions("test_"+selector, "result_"+selector );      
            trainGraph.validateResult("test_"+selector, "result_"+selector);               
            trainGraph.shutDownDB();
        }
    }
}
