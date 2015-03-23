/*
 * IQR (Interactive Query Relaxation) Library
 * Copyright (C) 2011  Davide Mottin (mottin@disi.unitn.eu
 * Alice Marascu (marascu@disi.unitn.eu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package it.unitn.disi.db.queryrelaxation.model;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.exceptions.DataException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a boolean database, it allows a maximum of 32 Attributes
 * @author Davide Mottin
 */
public class EfficientBoolDatabase {
    private double[] benefits;
    /*
     * The db represented by integer tuples (binary tuples)
     */
    private int[] db;
    /*
     * The index from attribute to tuples
     */
    private final Map<Integer,int[]> attributeToTuple;
    /*
     * Size of the database (number of attributes)
     */
    private int size;
    /*
     * The vector of attributes
     */
    private List<String> attributes;
    
    
    private double maxBenefit; 
    private double minBenefit; 
    
    public EfficientBoolDatabase(String path) throws FileNotFoundException, IOException, NumberFormatException, ConnectionException {
        size = 0;
        attributeToTuple = new HashMap<>();
        minBenefit = Double.MAX_VALUE;
        maxBenefit = -(Double.MAX_VALUE);
        populate(path);
    }

    private void populate(String pathToDB) throws
            NumberFormatException, ConnectionException 
    {
        String line;
        String[] splittedLine;
        List<Integer> data = new ArrayList<>();
        Map<Integer,List<Integer>> tmpAttTuple = new HashMap<>();
        List<Double> benefitList = new ArrayList<>();
        int tuple;
        int attValue;
        double benefit;
        int[] tuples; 
        List<Integer> matchTuples;
        int count = 0;
        
        try (BufferedReader br = new BufferedReader(new FileReader(pathToDB))) {
            while ((line = br.readLine()) != null) {
                splittedLine = line.split(" |\t");
                tuple = 0;
                count = 0;
                benefit = Double.parseDouble(splittedLine[0]);
                benefitList.add(benefit);
                if (benefit < minBenefit)
                    minBenefit = benefit;
                if (benefit > maxBenefit)
                    maxBenefit = benefit;
                for (int i = 1; i < splittedLine.length; i++) {
                    attValue = Integer.parseInt(splittedLine[i].trim());
                    if (attValue == 1) {
                        matchTuples = tmpAttTuple.get(count); //Contains the tuples per attribute value
                        if (matchTuples == null)
                            matchTuples = new ArrayList<>();
                        matchTuples.add(data.size());
                        tmpAttTuple.put(count, matchTuples);
                    }
                    tuple += (attValue << count++);
                }
                data.add(tuple);
            }//END WHILE
            size = count;
            if (size > Integer.SIZE)
                throw new ConnectionException(String.format("Cannot handle databases with more than %d attributes", Integer.SIZE));
            this.attributes = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                attributes.add(i + "");
            }
            for (Integer att : tmpAttTuple.keySet()) {
                matchTuples = tmpAttTuple.get(att);
                tuples = new int[matchTuples.size()];
                for (int i = 0; i < matchTuples.size(); i++) {
                    tuples[i] = matchTuples.get(i);
                }
                attributeToTuple.put(att, tuples);
            }
            db = new int[data.size()];
            benefits = new double[data.size()];
            for (int i = 0; i < data.size(); i++) {
                db[i] = data.get(i);
                benefits[i] = benefitList.get(i);
            }
            //benefits = benefitList.toArray(new Double[benefitList.size()]);
        } catch (IOException ex) {
            throw new ConnectionException(String.format("Input file %s cannot be read", pathToDB), ex);
        } catch (NumberFormatException ex) {
            throw new NumberFormatException("Cannot parse input file at line %d");
        } catch (Exception ex) {
            throw new ConnectionException(ex);
        }
    }

    public int resultSize(Query q, boolean restricted) {
        return 0;
    }
    
    public Pair<int[],double[]> resultSet(Query q, boolean restricted) {
        int query = 0, i, count = 0;
        int min = Integer.MAX_VALUE;
        int[] minList = null;
        int[] tmpRs, rs;
        double[] tmpBenefit, benefit; 
        int tuple;
        String name;
        if (q.size() == 0) 
            return new Pair<>(db,benefits);
        
        for (Constraint cons : q.getConstraints()) {
            name = cons.getAttributeName();
            for (i = 0; i < attributes.size(); i++) {
                if ((attributes.get(i)).equals(name)) {
                    if (attributeToTuple.containsKey(i)) {
                        if (attributeToTuple.get(i).length < min) {
                            minList = attributeToTuple.get(i);
                            min = minList.length;
                        }
                    }
                    /** BOOLEAN QUERY **/
                    query += (1 << i);
                }
            }//END FOR
        }//END FOR
        if (minList == null)
            return new Pair<>(new int[0], new double[0]);
            //throw new DataException(String.format("The input query %s cannot be performed in the database", q.toString()));
        tmpRs = new int[minList.length];
        tmpBenefit = new double[minList.length];
        
        for (i = 0; i < minList.length; i++) {
            tuple = db[minList[i]];
            if ((tuple & query) == query) {
                tmpRs[count] = tuple; 
                tmpBenefit[count] = benefits[minList[i]];
                count++;
            }
        }
        rs = new int[count];
        benefit = new double[count];
        System.arraycopy(tmpRs, 0, rs, 0, count);
        System.arraycopy(tmpBenefit, 0, benefit, 0, count);
        return new Pair<>(rs, benefit);
    }
    
    public Pair<int[], double[]> resultSet(Query q) {
        return resultSet(q, false);
    }
    
//    public List<Tuple> getTopKTuples(int[] weights, Query q, int k) {
//        List<Tuple> topk = new ArrayList<Tuple>();
//        int tuple ;
//        int[][] weightDb = new int[db.length][];  
//        int weight;
//        
//        for (int i = 0; i < db.length; i++) {
//            tuple = db[i];
//            weightDb[i] = new int[2];
//            weightDb[i][0] = tuple;
//            weight = 0;
//            for (int j = 0; j < size; j++) {                
//                if ((tuple & (1 << j)) > 0) {
//                    weight += weights[j]; 
//                }
//            }
//            weightDb[i][1] = weight;    
//        }
//        Arrays.sort(weightDb, new Comparator<int[]>() {
//            public int compare(int[] o1, int[] o2) {
//                if (o1[1] > o2[1])
//                    return -1;
//                if (o1[1] < o2[1])
//                    return 1;
//                return 0;
//            }
//        });
//        int query = q != null? toBooleanQuery(q) : -1;
//        for (int i = 0; i < k && i < weightDb.length; i++) {
//            topk.add(toTuple(weightDb[i][0], query));
//        }
//        return topk;
//    }
//        
//        
//    public List<Tuple> getTopKTuples(int[] weights, int k) {
//        return getTopKTuples(weights, null, k);
//    }

    private int toBooleanQuery(Query q) {
        int query = 0;
        String name;
        for (Constraint cons : q.getConstraints()) {
            name = cons.getAttributeName();
            for (int i = 0; i < attributes.size(); i++) {
                if ((attributes.get(i)).equals(name)) {
                    /** BOOLEAN QUERY **/
                    query += (1 << i);
                }
            }//END FOR
        }//END FOR
        return query;
    }
    
    
//    private Tuple toTuple(int tuple, int query) {
//        Tuple t = new Tuple();
//        for (int i = 0; i < size; i++) {
//            if ((tuple & (1 << i)) > 0)
//                t.addValue(i + "", true);
//            else if (query == -1)
//                t.addValue(i + "", false);
//        }
//        return t;
//    }
    
//    private Tuple toTuple(int tuple) {
//        return toTuple(tuple, -1);
//    }
    
    public int size(){
        return db.length;
    }

    public int dimension() {
        return size;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public int count(int attribute) {
        return attributeToTuple.get(attribute).length;
    }

    public int noAttributes(){//A: added
        if(attributes != null) return attributes.size();
        return 0;
    }

    public double getMaxBenefit() {
        return maxBenefit;
    }

    public double getMinBenefit() {
        return minBenefit;
    }

//    @Override 
//    public String toString(){
//        StringBuilder tmp = new StringBuilder();
//        if (db != null)
//            for(int i = 0; i < db.length; i++)
//              tmp.append(toTuple(db[i])).append("\n") ;
//        return tmp.toString();     
//    }
}
