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

package it.unitn.disi.db.queryrelaxation.statistics;

import eu.unitn.disi.db.mutilities.LoggableObject;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.VectorRandomQuery; //A:
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

/**
 * Generator of empty queries from the database
 * @author Alice Marascu
 */
public class EmptyQueryGeneration extends LoggableObject {

    public Vector generated_Queries = new Vector();
    public int[] countingNoGenQueries; // counts the number of each size query that was generated

    public Vector generateQueries1(DatabaseConnector db, int minSizeOfQueryToGen, int maxSizeOfQueryToGen, int noQueriesToGen,
            int maxNoOfNonVidesQueriesToTry, int noRepetitions, int noOfAttributes) throws Exception {
        //initial version that generates different sizes queries

        System.out.println("No of attributes of the db = " + noOfAttributes);
        if (minSizeOfQueryToGen > noOfAttributes || maxSizeOfQueryToGen > noOfAttributes) {
            throw new Exception("The min or max size of the requsted query is larger than the no of attributes of the db (=" + noOfAttributes + " attributes).");
        }

        Random ran = new Random();
        Vector generatedElem = new Vector(); //contains elem of type VectorRandomQuery
        VectorRandomQuery vec;
        int tmpRandom;
        List<Constraint> constraints2; // = new ArrayList<Constraint>();
        boolean nonVideQuery;
        int notFoundandRepetitionNoReached;
        int tmpMaxNoOfNonVidesQueriesToTry, jj, kk, ll, ii;
        Query q = null;
        Vector generatedQueries = new Vector();

        for (int sizeOfQuery = minSizeOfQueryToGen; sizeOfQuery <= maxSizeOfQueryToGen; sizeOfQuery++) {
            ll = 0;// no tries and found repeated
            ii = 0;
            while (ii < noQueriesToGen && ll < noRepetitions /* same value found or not found at all*/) {//System.out.println("try no: "+(ll-1));

                tmpMaxNoOfNonVidesQueriesToTry = 0;
                jj = 0; //no of repetitions to try
                do {

                    notFoundandRepetitionNoReached = 0;
                    vec = new VectorRandomQuery();
                    nonVideQuery = false;

                    constraints2 = new ArrayList<Constraint>();
                    outerloop:
                    for (int tmps = 0; tmps < sizeOfQuery; tmps++) {

                        //generate query of size sizeOfQuery
                        tmpRandom = ran.nextInt(noOfAttributes);
                        jj = 0;
                        if (vec.query_elem.size() != 0) {
                            while (vec.query_elem.contains(new Integer(tmpRandom))) {

                                if (jj++ > noRepetitions) {
                                    notFoundandRepetitionNoReached = 1;
                                    // System.out.print("Repeated:"+tmpRandom+ " in vec:"+vec.query_elem);
                                    break outerloop;
                                }
                                tmpRandom = ran.nextInt(noOfAttributes);

                            }
                        }

                        vec.query_elem.add(new Integer(tmpRandom));
                        constraints2.add(new Constraint(tmpRandom + "", true));
                    }
                    //System.out.println("Generated constraints: "+constraints2.toString());
                    //check whether this numbers combination exists already
                    for (kk = 0; kk < generatedElem.size(); kk++) {
                        if (((VectorRandomQuery) generatedElem.get(kk)).haveSameValues(vec)) {
                            ll++;
                            //   System.out.println("same values");
                            break;
                        }
                    }
                    if (kk == generatedElem.size()) { // good generated
                        //check if empty query
                        q = new Query(constraints2);
                        if (db.submitQuery(q).length != 0) {
                            //   System.out.println("Query" +q.toString()+" is not empty.");
                            nonVideQuery = true;
                            tmpMaxNoOfNonVidesQueriesToTry++;
                        } else {
                            if (notFoundandRepetitionNoReached != 1) {
                                //query is good
                                vec.sortTheValues(); //System.out.println("Sorted :"+vec.toString());
                                generatedElem.add(vec);
                                q = new Query(vec.transf());
                                generatedQueries.add(q);
                                ii++;
                            }
                        }
                    }

                    if (tmpMaxNoOfNonVidesQueriesToTry == maxNoOfNonVidesQueriesToTry) {
                        //  System.out.println("the max number of non vide queries to try was reached");
                        q = null;
                        ll++;//increase the no of failed tries
                    }
                } while (nonVideQuery && tmpMaxNoOfNonVidesQueriesToTry < maxNoOfNonVidesQueriesToTry);
            }
        }
        debug("Query generation result:");
        for (int iiii = 0; iiii < generatedQueries.size(); iiii++) {
            debug(((Query) generatedQueries.get(iiii)).toString());
        }

        return generatedQueries;
    }

    public Vector generateQueries(DatabaseConnector db, int minSizeOfQueryToGen, int maxSizeOfQueryToGen, int noQueriesToGen,
            int maxNoOfNonVidesQueriesToTry, int noRepetitions, int noOfAttributes) throws Exception 
    {
// modified version, it starts from the maximum size and eliminates attributes one by one.
        info("No of attributes of the db = " + noOfAttributes);
        if (minSizeOfQueryToGen > noOfAttributes) {
            throw new Exception("The min(" + minSizeOfQueryToGen + ") size of the requsted query is larger than the no of attributes of the db (=" + noOfAttributes + " attributes).");
        }  
        if (maxSizeOfQueryToGen > noOfAttributes) {
            maxSizeOfQueryToGen = noOfAttributes;
            warn("The max(%d) size is above the number of Attributes (%d), it will be set to (%d)", maxSizeOfQueryToGen, noOfAttributes, noOfAttributes);
        }

        Random ran = new Random();
        Vector generatedElem = new Vector(); //contains elem of type VectorRandomQuery
        VectorRandomQuery vec;
        int tmpRandom;
        List<Constraint> constraints2; // = new ArrayList<Constraint>();
        boolean nonVideQuery;
        int notFoundandRepetitionNoReached;
        int tmpMaxNoOfNonVidesQueriesToTry, jj, kk, ll, ii;
        Query q = null;
        Vector generatedQueries = new Vector();

//    for(int sizeOfQuery = minSizeOfQueryToGen; sizeOfQuery <= maxSizeOfQueryToGen; sizeOfQuery++) {

        int sizeOfQuery = maxSizeOfQueryToGen;
        ll = 0;// no tries and found repeated
        ii = 0;
        while (ii < noQueriesToGen && ll < noRepetitions /* same value found or not found at all*/) {//System.out.println("try no: "+(ll-1));

            tmpMaxNoOfNonVidesQueriesToTry = 0;
            jj = 0; //no of repetitions to try
            do {

                notFoundandRepetitionNoReached = 0;
                vec = new VectorRandomQuery();
                nonVideQuery = false;

                constraints2 = new ArrayList<Constraint>();
                outerloop:
                for (int tmps = 0; tmps < sizeOfQuery; tmps++) {

                    //generate query of size sizeOfQuery
                    tmpRandom = ran.nextInt(noOfAttributes);
                    jj = 0;
                    if (vec.query_elem.size() != 0) {
                        while (vec.query_elem.contains(new Integer(tmpRandom))) {

                            if (jj++ > noRepetitions) {
                                notFoundandRepetitionNoReached = 1;
                                // System.out.print("Repeated:"+tmpRandom+ " in vec:"+vec.query_elem);
                                break outerloop;
                            }
                            tmpRandom = ran.nextInt(noOfAttributes);

                        }
                    }

                    vec.query_elem.add(new Integer(tmpRandom));
                    constraints2.add(new Constraint(tmpRandom + "", true));
                }
                //System.out.println("Generated constraints: "+constraints2.toString());
                //check whether this numbers combination exists already
                for (kk = 0; kk < generatedElem.size(); kk++) {
                    if (((VectorRandomQuery) generatedElem.get(kk)).haveSameValues(vec)) {
                        ll++;
                        //   System.out.println("same values");
                        break;
                    }
                }
                if (kk == generatedElem.size()) { // good generated
                    //check if empty query
                    q = new Query(constraints2);
                    if (db.submitQuery(q).length != 0) {
                        //   System.out.println("Query" +q.toString()+" is not empty.");
                        nonVideQuery = true;
                        tmpMaxNoOfNonVidesQueriesToTry++;
                    } else {
                        if (notFoundandRepetitionNoReached != 1) {
                            //query is good
                            vec.sortTheValues(); //System.out.println("Sorted :"+vec.toString());
                            generatedElem.add(vec);
                            q = new Query(vec.transf());
                            generated_Queries.add(q);
                            ii++;
                        }
                    }
                }

                if (tmpMaxNoOfNonVidesQueriesToTry == maxNoOfNonVidesQueriesToTry) {
                    //  System.out.println("the max number of non vide queries to try was reached");
                    q = null;
                    ll++;//increase the no of failed tries
                }
            } while (nonVideQuery && tmpMaxNoOfNonVidesQueriesToTry < maxNoOfNonVidesQueriesToTry);
        }
        //  }

        //clone the generated_Queries vector
        Vector tmpGenerated_Queries = new Vector();
        for (int iiii = 0; iiii < generated_Queries.size(); iiii++) {
            tmpGenerated_Queries.add((Query) ((Query) generated_Queries.get(iiii)).clone());
        }

        countingNoGenQueries = new int[maxSizeOfQueryToGen + 1];

        for (int iiii = 0; iiii < tmpGenerated_Queries.size(); iiii++) {
            if (maxSizeOfQueryToGen > minSizeOfQueryToGen) {
                decrease(minSizeOfQueryToGen, db, ((Query) tmpGenerated_Queries.get(iiii)), noQueriesToGen);
            }
        }

        System.out.println("\nQuery generation result:");
        for (int iiii = 0; iiii < generated_Queries.size(); iiii++) {
            System.out.println((Query) generated_Queries.get(iiii));
        }

        return generated_Queries;
    }

    public void decrease(int minSizeQuery, DatabaseConnector db, Query queryToDecrease, int noQueriesToGen) throws Exception {
        // if to generate all possible queries
        Query clona = (Query) queryToDecrease.clone();
        for (int i = 0; i < queryToDecrease.size() && countingNoGenQueries[(int) clona.size() - 1] < noQueriesToGen; i++) {
            clona = (Query) queryToDecrease.clone();
            clona.remove(i);
            if (db.submitQuery(clona).length == 0) {

                if (countingNoGenQueries[(int) clona.size()] < noQueriesToGen) {
                    generated_Queries.add(clona);//avoids concurrent thread
                }
                countingNoGenQueries[(int) clona.size()]++;
                if (clona.size() > minSizeQuery && countingNoGenQueries[(int) clona.size() - 1] < noQueriesToGen) {
                    decrease(minSizeQuery, db, clona, noQueriesToGen);
                }
            }

        }
    }

    public static Vector<Query> generateQuery(File queryFile, int minQuerySize, int maxQuerySize) throws IOException {
        BufferedReader queryFileReader = null;
        String line = null;
        Vector<Query> queries = new Vector<Query>();
        Query q;
        int count = 0;
        
        try {
            
            queryFileReader = new BufferedReader(new FileReader(queryFile));
            while ((line = queryFileReader.readLine()) != null) {
                count++;
                line = line.trim();
                if (line.length() != 0) {//Non empty line
                    q = stringToQuery(line);
                    if (q.size() >= minQuerySize && q.size() <= maxQuerySize) {
                        queries.add(q);
                    }
                }
            }
        } catch (IOException ex) {
            throw new IOException(String.format("Malformed query file, computations stopped at line %d", count), ex);
        } finally {
            if (queryFileReader != null)
                try {
                    queryFileReader.close();
                } catch (IOException ex) {
                }
        }
        return queries;
    }
    
    public static Query stringToQuery(String q) {
        Query query;
        String[] splittedLine = q.split(",");
        query = new Query();
        for (String cons : splittedLine) {
            query.addConstraint(new Constraint(cons, true));
        }
        return query;
    }
    
    public static String queryToString(Query q) {
        String query = "";
        for (Constraint c : q.getConstraints()) {
            query += c.getAttributeName() + ",";
        }
        return !query.isEmpty()? query.substring(0, query.length() - 1) : "";
    }
}
