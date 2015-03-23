/*
 * Copyright (C) 2014 Davide Mottin <mottin@disi.unitn.eu>
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
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package it.unitn.disi.db.queryrelaxation.commands;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.global.Command;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.BooleanMockConnector;
import it.unitn.disi.db.queryrelaxation.model.functions.IPFPrior;
import it.unitn.disi.db.queryrelaxation.model.functions.IdfFunction;
import it.unitn.disi.db.queryrelaxation.statistics.EmptyQueryGeneration;
import it.unitn.disi.db.queryrelaxation.tree.ConvolutionTree;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationTree;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO: Refine this class
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TestThroughput extends Command {
    private String pathToDb;
    private int n; 
    private int minQuerySize; 
    private int maxQuerySize;
    private int cardinality;
    private int[] algorithms;
    private int t; 
    
    @Override
    protected void execute() throws eu.unitn.disi.db.command.exceptions.ExecutionException {
        executeThroughput(pathToDb, minQuerySize, maxQuerySize, n, t, cardinality);
    
    }

    @Override
    protected String commandDescription() {
        return "Test the algorithms with a set of parallel queries"; 
    }
    
    private static class ThroughputProcess implements Callable<Long> {
        private RelaxationTree tree;
        
        public ThroughputProcess(RelaxationTree tree) {
            this.tree = tree;
        }
        
        public Long call() throws Exception {
            tree.materialize(true);
            return tree.getTime();
        }
    }
    
    public static void executeThroughput(String pathToDb, int minQuerySize, int maxQuerySize, int n, int t, int cardinality) {
        Query q; 
        BufferedReader testReader = null;
        String line;
        String[] splittedLine;
        IPFPrior prior;
        PreferenceFunction pref;
        BooleanMockConnector db;
        RelaxationTree tree;
        long totalTime; 
        RelaxationTree.TreeType type = RelaxationTree.TreeType.MIN_EFFORT;
        int L = 3; 
        int buckets = 20;
        ExecutorService pool = Executors.newCachedThreadPool();
        TreeMap<Integer,List<Pair<Query,String>>> sizeQueryMap; 
        Map<String,BooleanMockConnector> dbs; 
        Map<String,IPFPrior> ipfs; 
        List<Pair<Query,String>> queries; 
        Pair<Query,String> data;
        
        
        try {
            testReader = new BufferedReader(new FileReader(pathToDb));
            System.out.printf("Successfully Loaded file: %s\n-----------------\n", pathToDb);
            sizeQueryMap = new TreeMap<>();
            ipfs = new HashMap<>();
            dbs = new HashMap<>();
            
            while ((line = testReader.readLine()) != null) {
                line = line.trim();
                
                //Load the queries and repeat them a fixed amount of times. 
                if (line.length() != 0) {
                    splittedLine = line.split("\t");
                    q = EmptyQueryGeneration.stringToQuery(splittedLine[2]);
                    if (q.size() >= minQuerySize && q.size() <= maxQuerySize) {
                        System.out.printf("Processing query: %s\ndb: %s\nIPF: %s\n", q.toString(), splittedLine[0], splittedLine[1]);
                        try {
                        } catch (Exception ex) {
                            Logger.getLogger(TestFramework.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        queries = sizeQueryMap.get((int)q.size());
                        if (queries == null) {
                            queries = new ArrayList<>();
                        }
                        queries.add(new Pair<>(q, splittedLine[0] + "," + splittedLine[1]));
                        sizeQueryMap.put((int)q.size(), queries);
                        db = dbs.get(splittedLine[0]);
                        prior = ipfs.get(splittedLine[1]);
                        if (db == null) {
                            db = new BooleanMockConnector(splittedLine[0]);
                            db.connect();
                        }
                        dbs.put(splittedLine[0], db);
                        ipfs.put(splittedLine[1], prior);
                    }
                }
            }
            System.out.println("Loaded all the databases and ipfs into memory, now let's start the computation");
            List<Future<Long>> times; 
            //looping over all the sizes in ascending order
            for (Integer size : sizeQueryMap.keySet()) {
                queries = sizeQueryMap.get(size);
                times = new ArrayList<>();
                //Submit the batches and iterate over the list
                for (int i = 0; i < n; i++) {
                    data = queries.get(i % queries.size()); //Iterate over the queries several tiems
                    splittedLine = data.getSecond().split(",");
                    db = dbs.get(splittedLine[0]);
                    prior = new IPFPrior(db, splittedLine[1], data.getFirst());
//                    prior = ipfs.get(splittedLine[1]);
                    pref = new IdfFunction(db);
                    tree = new ConvolutionTree(data.getFirst(), L, buckets, cardinality, type); //Only for convolution if not required
                    tree.setDb(db);
                    tree.setPref(pref);
                    tree.setPrior(prior);
                    tree.setVerbose(false);
                    
                    times.add(pool.submit(new ThroughputProcess(tree)));
                    if (db.submitQuery(data.getFirst()).length != 0) {
                        System.out.println("The query is not empty.");
                        return;
                    }
                }
                totalTime = 0L;
                for (Future<Long> timeFuture : times) {
                    totalTime += timeFuture.get();
                }
                System.out.printf("Total time for %d batches of query size %d is %dms\n", n, size, totalTime);
            }    
        } catch (InterruptedException | ExecutionException | ConnectionException ex) {
            Logger.getLogger(TestFramework.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TestFramework.class.getName()).log(Level.SEVERE, "An error occurred in reading the test File, message follows", ex);
        } finally {
            pool.shutdown();
        }       
    }

    
    @CommandInput(
        consoleFormat = "-db",
        defaultValue = "",
        mandatory = true,
        description = "folder containing the database to be used")
    public void setDbFolder(String dbFolder) {
        this.pathToDb = dbFolder;
    }


    @CommandInput(
        consoleFormat = "-m",
        defaultValue = "3",
        mandatory = false,
        description = "minimum query size to be processed")    
    public void setMinQuerySize(int minQuerySize) {
        this.minQuerySize = minQuerySize;
    }

    @CommandInput(
        consoleFormat = "-M",
        defaultValue = "7",
        mandatory = false,
        description = "maximum query size to be processed")    
    public void setMaxQuerySize(int maxQuerySize) {
        this.maxQuerySize = maxQuerySize;
    }

    @CommandInput(
        consoleFormat = "-c",
        defaultValue = "1",
        mandatory = false,
        description = "minimum cardinality for a set of resutls") 
    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }
    
    @CommandInput(
        consoleFormat = "-t",
        defaultValue = "",
        mandatory = true,
        description = "algorithms to be used in the evaluation") 
    public void setAlgorithms(int[] algorithms) {
        this.algorithms = algorithms;
    }

    @CommandInput(
        consoleFormat = "-n",
        defaultValue = "10",
        mandatory = true,
        description = "number of threads") 
    public void setN(int n) {
        this.n = n;
    }
}
