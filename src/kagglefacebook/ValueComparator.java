/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package kagglefacebook;

import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author rafael
 */
public class ValueComparator implements Comparator{
       
       Map base;       
       public ValueComparator(Map base){
           this.base = base;
       }              

        @Override
        public int compare(Object a, Object b) {
            Float fa = (Float) base.get(a);
            Float fb = (Float) base.get(b);
            if (fa<fb)
                return 1;
            else if(fa==fb)
                return 0;
            else
                return -1;
        }
       
   }