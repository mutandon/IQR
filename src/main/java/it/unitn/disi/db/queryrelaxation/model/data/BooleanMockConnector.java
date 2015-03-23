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
package it.unitn.disi.db.queryrelaxation.model.data;

import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.EfficientBoolDatabase;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.Query;
import java.util.HashMap;
import java.util.Map;

/**
 * Connects to a fake dataset that is a boolean hard-coded matrix.
 * @author Davide Mottin
 */
public class BooleanMockConnector implements DatabaseConnector {
    /*
     * A boolean database: each row is a tuple; efficient implementation to 
     * reduce the time blow up
     */
    private EfficientBoolDatabase database; 
    /*
     * Actual path of the db
     */
    private String dbPath;
    /*
     * Total time to interrogate the database
     */
    private long totalTimeDbInterrogation; 
    /*
     * The matrix of idfs
     */
    private Map<Integer,Pair<Double,Double>> idfs;
    

    private boolean isConnected = false;
    
    public BooleanMockConnector(String path) throws
            java.io.FileNotFoundException, java.io.IOException, NumberFormatException, ConnectionException {//A:
        dbPath = path;
        totalTimeDbInterrogation = 0;
    }

    public boolean connect() throws ConnectionException {
        if (!isConnected) {
            try {
                database = new EfficientBoolDatabase(dbPath);
                idfs = new HashMap<>();
                int count;
                for (int i = 0; i < database.noAttributes(); i++) {
                    try {
                        count = 1 + database.count(i);
                    } catch (NullPointerException nex) {
                        count = 1;
                    }
                    idfs.put(i,new Pair<Double,Double>(Math.log10(database.size()/(double)count),Math.log10(database.size()/(database.size() - (double)count))));
                }
                isConnected = true;
            } catch (Exception ex) {
                throw new ConnectionException(ex);
            }
        }
        return true;
    }

    @Override
    public boolean close() throws ConnectionException {
        isConnected = false;
        return isConnected;
    }

    @Override
    public int[] submitQuery(Query q) throws ConnectionException {
        return submitQuery(q, false);
    }

    public int[] submitQuery(Query q, boolean restricted) throws ConnectionException {
        long curentTime =  System.nanoTime();
        if (database == null) {
            throw new ConnectionException("Database is not connected");
        } 
        int[] result = null;
        result = database.resultSet(q).getFirst();
        totalTimeDbInterrogation +=  System.nanoTime() - curentTime; 
        return result;
    }

    
    @Override
    public int count(int attIndex, Object value) throws ConnectionException {
        int c = 0;
        if (database == null) {
            throw new ConnectionException("Database is not connected");
        } 
        if (attIndex >= database.getAttributes().size()) 
        {
            throw new ConnectionException(String.format("Attribute index %d does not exist", attIndex));
        }
        try {
            c = database.count(attIndex);
        } catch (Exception ex) {
            throw new ConnectionException(String.format("Attribute %d does not exist", attIndex));
        }
        return c;
    }

//    public int[] queryTupleSpace(Query q, boolean negatives) throws ConnectionException {
//        if (database == null) {
//            throw new ConnectionException("Database is not connected");
//        } 
//        //List<Tuple> result;
//        List<Constraint> constraints = q.getConstraints();
//        if (negatives) {
//            constraints.addAll(q.negatedConstraints());
//        }
//        Map<String, Integer> names = new HashMap<String, Integer>();
//        List<Pair<Integer, List<Object>>> possibleValues = new ArrayList<Pair<Integer, List<Object>>>();
//        List<Object> values;
//        List<String> attributes = database.getAttributes();
//        
//        //This implementation works well only with boolean databases
//        for (int i = 0; i < constraints.size(); i++) {
//            names.put(constraints.get(i).getAttributeName(), i);
//        }
//
//        for (int i = 0; i < attributes.size(); i++) {
//            values = new ArrayList<>();
//            if (names.containsKey((String) attributes.get(i))) {
//                values.add(constraints.get(names.get((String) attributes.get(i))).getValue());
//                possibleValues.add(new Pair(i, values));
//            } else {
//                values.add(true); //WARNING: ONLY FOR BOOLEAN DBS
//                values.add(false);
//                possibleValues.add(new Pair(i, values));
//            }
//        }
//        //DO the cartesian product between all values
//        result = cartesianProduct(possibleValues);
//        return result;
//    }

    @Override
    public int size() throws ConnectionException {
        if (database == null) {
            throw new ConnectionException("Database is not connected");
        } 
        return database.size();  //A:
    }

    @Override
    public String[] getAttributeNames() throws ConnectionException {
        if (database == null) {
            throw new ConnectionException("Database is not connected");
        } 
        String[] attr = new String[database.getAttributes().size()];  //A:
        for (int i = 0; i < database.getAttributes().size(); i++) {
            attr[i] = (String) database.getAttributes().get(i);
        }
        return attr;
    }

    /* query and tuple must be represented by numbers */
//    private Tuple numberToTuple(int num, int query){
//        Tuple t = new Tuple();
//        int mask = 1;
//        int pointer = 1;
//        for (int i = 0; i < ATTRIBUTES.length; i++) {
//            if ((mask & query) == mask) {
//                t.addValue(ATTRIBUTES[i], true);
//            } else {
//                t.addValue(ATTRIBUTES[i], (pointer & num) == pointer);
//                pointer <<= 1;
//            }
//            mask <<= 1;
//        }
//        return t;
//    }
//    private List<Tuple> cartesianProduct(List<Pair<Integer, List<Object>>> possibleValues) {
//        List<Tuple> retval = new ArrayList<Tuple>();
//        List<Object> values;
//        Tuple t;
//        List<String> attributes = database.getAttributes();
//        if (possibleValues == null || possibleValues.isEmpty()) {
//            return null;
//        } else if (possibleValues.size() == 1) {
//            values = possibleValues.get(0).getSecond();
//            for (int i = 0; i < values.size(); i++) {
//                t = new Tuple();
//                t.addValue((String)attributes.get(possibleValues.get(0).getFirst()), values.get(i)); //A:
//                //t.addValue(ATTRIBUTES[possibleValues.get(0).getFirst()], values.get(i));
//                retval.add(t);
//            }
//            return retval;
//        } else {
//            return update(possibleValues.get(0), cartesianProduct(possibleValues.subList(1, possibleValues.size())));
//        }
//    }

//    public List<Tuple> topkTuples(int[] weights, Query q, int k) throws ConnectionException {        
//        if (database == null) {
//            throw new ConnectionException("Database is not connected");
//        } 
//        List<Tuple> result = null;
//        try {
//            result = database.getTopKTuples(weights, q, k);
//        } catch (Exception ex) {
//            throw new ConnectionException(ex);
//        }
//        return result;
//    }
//    
//    public List<Tuple> topkTuples(int[] weights, int k) throws ConnectionException {
//        return topkTuples(weights, null, k);
//    }
    
//    private List<Tuple> update(Pair<Integer, List<Object>> initialList, List<Tuple> addingList) {
//        List<Tuple> result = new ArrayList<Tuple>();
//        Tuple t;
//        Integer index = initialList.getFirst();
//        List<Object> values = initialList.getSecond();
//        List<String> attributes = database.getAttributes();
//        for (int i = 0; i < values.size(); i++) {
//            for (int j = 0; j < addingList.size(); j++) {
//                t = new Tuple();
//                t.addValue((String)attributes.get(index), values.get(i)); //A:
//                //t.addValue(ATTRIBUTES[index], values.get(i));
//                t.merge(addingList.get(j));
//                result.add(t);
//            }
//        }
//        return result;
//    }


    @Override
    public int getAttributeNumber(){//A: added
        if(database != null) {
            return database.noAttributes();
        }
        return 0;
    }

    public long getTotalTimeDbInterrogation(){ //A:
        return this.totalTimeDbInterrogation;
    }

    public void resetTime(){
        this.totalTimeDbInterrogation = 0;
    }

    @Override
    public double idf(int attIndex, Object value) throws ConnectionException {
        if (value.equals(true)) {
            return idfs.get(attIndex).getFirst();
        }
        return idfs.get(attIndex).getSecond();
    }

    @Override
    public double getMinBenefit() throws ConnectionException {
        return database.getMinBenefit();
    }

    @Override
    public double getMaxBenefit() throws ConnectionException {
        return database.getMaxBenefit();
    }

    @Override
    public Pair<Double,Double> getMinMaxBenefit(Query q) throws ConnectionException {
        Pair<int[],double[]> results = database.resultSet(q);
        Pair<Double, Double> lbub;
        int[] rs = results.getFirst();
        double[] benefits = results.getSecond();
        double min = Double.MAX_VALUE;
        double max = 0;

        double benefit;
        for (int i = 0; i < rs.length; i++) {
            benefit = benefits[i];
            if (benefit < min) {
                min = benefit;
            }
            if (benefit > max) {
                max = benefit;
            }
        }
        lbub = new Pair<>(min, max);
        return lbub;
    }

    @Override
    public boolean isConnected() {
        return isConnected || database == null;
    }

    @Override
    public Pair<int[], double[]> resultsAndBenefits(Query q) throws ConnectionException {
        return database.resultSet(q);
    }

}
