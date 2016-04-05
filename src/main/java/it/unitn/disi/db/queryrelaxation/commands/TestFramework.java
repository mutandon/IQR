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
package it.unitn.disi.db.queryrelaxation.commands;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.mutilities.StopWatch;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Prior;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.BooleanMockConnector;
import it.unitn.disi.db.queryrelaxation.model.functions.DatabaseFunction;
import it.unitn.disi.db.queryrelaxation.model.functions.IPFPrior;
import it.unitn.disi.db.queryrelaxation.model.functions.IdfFunction;
import it.unitn.disi.db.queryrelaxation.statistics.EmptyQueryGeneration;
import it.unitn.disi.db.queryrelaxation.tree.topk.TopKPruningTree;
import it.unitn.disi.db.queryrelaxation.tree.ConvolutionTree;
import it.unitn.disi.db.queryrelaxation.tree.GreedyRelaxationRandomChoiceTree;
import it.unitn.disi.db.queryrelaxation.tree.GreedyRelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.HeuristicPruningTree;
import it.unitn.disi.db.queryrelaxation.tree.OptimalRelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.comparison.QueryRefinementTree;
import it.unitn.disi.db.queryrelaxation.tree.RandomRelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationTree.TreeType;
import it.unitn.disi.db.queryrelaxation.tree.comparison.InteractiveMinimumFailing;
import it.unitn.disi.db.queryrelaxation.tree.topk.TopKConvolutionPruningTree;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This command perform test over the framework except for top-k experiments.
 *
 * @author Davide Mottin
 */
public class TestFramework extends Command {
    private String dbFolder;
    private String queryFile;
    private String preferenceFunction;
    private String optimizationCriteria;
    private String outputFile;
    private int minQuerySize;
    private int maxQuerySize;
    private int level;
    private int cardinality;
    private int buckets;
    private int k; 
    private int[] algorithms;
    private boolean writeTrees;
    private boolean compareObjectives;
    private boolean biased;

    @Override
    protected void execute() throws eu.unitn.disi.db.command.exceptions.ExecutionException {
        BooleanMockConnector db;
        Query q = null;
        TreeType type = TreeType.valueOf(optimizationCriteria);;
        
        try (BufferedReader testReader = new BufferedReader(new FileReader(queryFile))) {
            File directory = new File(dbFolder);
            if (directory == null) {
                throw new ExecutionException("Error in opening the directory.");
            }
            if (directory.list() == null) {
                throw new ExecutionException("Error: the directory %s does not contains anything.", directory);
            }

            info("Cardinality: %d", cardinality);
            info("Successfully Loaded file: %s", queryFile);
            
            String line = null;
            String[] splittedLine;
            while ((line = testReader.readLine()) != null) {
                line = line.trim();
                if (line.length() != 0 && !line.startsWith("#")) {
                    splittedLine = line.split("\t");
                    q = EmptyQueryGeneration.stringToQuery(splittedLine[2]);
                    if (q.size() >= minQuerySize && q.size() <= maxQuerySize) {
                        info("Processing query: %s\ndb: %s\nIPF: %s", q.toString(), splittedLine[0], splittedLine[1]);
                        execute(splittedLine[0], splittedLine[1], q, type);
                    }

                }
            }
        } catch (IOException ex) {
            throw new ExecutionException("Error while reading the input query file");
        }
    }

    @Override
    protected String commandDescription() {
        return "Execute experiments on the framework";
    }

    private void execute(String pathToDb, String pathToIPF, Query q, TreeType type) {
        int typeOfTree;
        int relaxationNodes;
        long buildingTime;
        long ipfTime;
        long queryTime;
        short failing;
        double profit = -1, answers = -1, effort = -1, expectedRelaxations;

        RelaxationTree tree = null, optTree = null;
        BufferedWriter br = null, out = null;
        BooleanMockConnector db;
        Prior prior;
        PreferenceFunction pref;
        String nameOfTree;
        String tmp = "";
        String query;
        DecimalFormat nf = new DecimalFormat("#.#");
        Query queryCopy;
        //TreeType optTreeType = null;
        StopWatch watch = new StopWatch();

        if (k > 1) {
            warn("k is ignored if the tree is not FastOpt or FastCDR, the others are not implemented");
        }
        
        nf.setMaximumFractionDigits(4);
        nf.setMinimumIntegerDigits(1);
        nf.setGroupingUsed(false);

        try  {
            watch.start();
            db = new BooleanMockConnector(pathToDb);
            db.connect();
            if ("IdfFunction".equals(preferenceFunction)) {
                pref = new IdfFunction(db);
            } else if ("DatabaseFunction".equals(preferenceFunction)) {
                pref = new DatabaseFunction(db);
            } else {
                try {
                    pref = (PreferenceFunction) Class.forName("it.unitn.disi.db.queryrelaxation.model.functions." + preferenceFunction).newInstance();
                } catch (Exception ex) {
                    warn("Failed to load %s with the canonical package it.unitn.disi.db.queryrelaxation.model.functions, trying to load as a fully qualified name");
                    pref = (PreferenceFunction) Class.forName(preferenceFunction).newInstance();
                }
            }

            prior = new IPFPrior(db, pathToIPF, q); //Prior does not change over the texts
            info("Loaded db and ipf in %dms", watch.getElapsedTimeMillis());

            if (db.submitQuery(q).length != 0) {
                error("The query is not empty.");
                return;
            }

            //Check different algorithms
            for (int i = 0; i < algorithms.length; i++) {
                typeOfTree = algorithms[i];
                failing = 1;

                switch (typeOfTree) {
                    case 0: //Random Relaxation Tree
                        tree = new RandomRelaxationTree(q, cardinality, type);
                        nameOfTree = "Rand";
                        break;
                    case 1: //Greedy Relaxation Tree
                        tree = new GreedyRelaxationTree(q, cardinality, type);
                        nameOfTree = "Greedy";
                        break;
                    case 2: //Greedy Random Relaxation Tree
                        tree = new GreedyRelaxationRandomChoiceTree(q, cardinality, type);
                        nameOfTree = "Greedy-Rand";
                        break;
                    case 3: //Brute Force Relaxation Tree
                        tree = new OptimalRelaxationTree(q, cardinality, type, k, biased);
                        nameOfTree = "FullTree";
                        break;
                    case 4: // Pruning Relaxation Tree
                        tree = new TopKPruningTree(q, cardinality, type, k, biased);
                        nameOfTree = "FastOpt";
                        break;
                    case 5://Heuristic Pruning Relaxation Tree Strategy.DIFFFIRST
                        tree = new HeuristicPruningTree(q, cardinality, type, HeuristicPruningTree.Strategy.DIFFFIRST);
                        nameOfTree = "FastOpt-Diff";
                        break;
                    case 6://Heuristic Pruning Relaxation Tree Strategy.LBFIRST
                        tree = new HeuristicPruningTree(q, cardinality, type, HeuristicPruningTree.Strategy.LBFIRST);
                        nameOfTree = "FastOpt-LB";
                        break;
                    case 7://Heuristic Pruning Relaxation Tree Strategy.UBFIRST
                        tree = new HeuristicPruningTree(q, cardinality, type, HeuristicPruningTree.Strategy.UBFIRST);
                        nameOfTree = "FastOpt-UB";
                        break;
                    case 8: //CDR 
                        tree = new ConvolutionTree(q, level, buckets, cardinality, type);
                        nameOfTree = "CDR";
                        break;
                    case 9: //FastCDR
                        tree = new TopKConvolutionPruningTree(q, level, buckets, cardinality, type, k, biased);
                        nameOfTree = "FastCDR";
                        break;
                    case 10: //Koudas paper
                        tree = new QueryRefinementTree(q);
                        nameOfTree = "QueryRef";
                        break;
                    case 11: 
                        tree = new InteractiveMinimumFailing(q); 
                        nameOfTree = "MFS";
                        break;
                    default:
                        System.err.println("wrong parameter");
                        return;
                }

                tree.setDb(db);
                tree.setPref(pref);
                tree.setPrior(prior);
                tree.setVerbose(false);

                buildingTime = -System.currentTimeMillis();
                tree.materialize(true);
                buildingTime += System.currentTimeMillis();
                queryTime = tree.getTime();

                info("Tree of type: %s, root cost: %f, number of nodes: %d, query time: %d", 
                        nameOfTree, tree.getRoot().getCost(), tree.getNumberOfNodes(), queryTime);
                info("Expected number of relaxations: %f", tree.expectedRelaxations());

                
                //File output = new File("OutputData");
                //output.mkdir();
                query = "";
                //measure failing queries
                for (Constraint c : q.getConstraints()) {
                    query += c.getAttributeName() + "_";
                    queryCopy = (Query) q.clone();
                    queryCopy.relax(c);
                    if (db.submitQuery(queryCopy).length != 0) {
                        failing = 0;
                    }
                }
                if (writeTrees) {
                    br = new BufferedWriter(new FileWriter(String.format("OutputData%stree%d_%s.dot", File.separator, typeOfTree, query)));
                    br.append(tree.toString());
                    br.close();
                }
                
                expectedRelaxations = tree.expectedRelaxations();
                //Compute optimal trees
                if (compareObjectives) {
                    optTree = tree.optimalTree(TreeType.MIN_EFFORT);
                    optTree.computeCosts();
                    effort = optTree.getRoot().getCost() / optTree.getRoot().getChildren().size();
                    optTree = tree.optimalTree(TreeType.PREFERRED);
                    optTree.computeCosts();
                    answers = optTree.getRoot().getCost() / optTree.getRoot().getChildren().size();
                    optTree = tree.optimalTree(TreeType.MAX_VALUE_MAX);
                    optTree.computeCosts();
                    profit = optTree.getRoot().getCost() / optTree.getRoot().getChildren().size();
                }
                
                if (writeTrees) {
                    br = new BufferedWriter(new FileWriter(String.format("OutputData%sopt_tree%d_%s.dot", File.separator, typeOfTree, query)));
                    optTree = tree.optimalTree((type == TreeType.MAX_VALUE_MAX || type == TreeType.PREFERRED) ? TreeType.MIN_EFFORT : TreeType.MAX_VALUE_MAX);
                    optTree.computeCosts();
                    br.append(optTree.toString());
                    br.close();
                }

                out = new BufferedWriter(new FileWriter(outputFile, true));

                info("Query time: %d, Building time: %d", queryTime, buildingTime);
                //Old comparison with optimal
                if (typeOfTree == 8 || typeOfTree == 9) {
                    info("No steps: " + ((ConvolutionTree) tree).getNumberOfSteps());
                }

                tmp += typeOfTree + "\t";
                tmp += nameOfTree + "\t";
                tmp += q.toString() + " \t"; // the query
                tmp += (int) q.size() + "\t"; //size of the query
                if (pathToIPF.contains("yahoo")) {
                    tmp += "yahoo\t";
                } else if (pathToIPF.contains("realtor")) {
                    tmp += "realtor\t";
                } else {
                    System.out.println("The name of the ipf file do not contain yahoo or realtor");
                }

                relaxationNodes = tree instanceof OptimalRelaxationTree ? ((OptimalRelaxationTree) tree).getRelaxationNodes() : 0;
                ipfTime = tree instanceof OptimalRelaxationTree ? ((OptimalRelaxationTree) tree).getTotalTimeIPFInterrogation() : 0;

                tmp += //no of tuples
                        pathToIPF.substring(pathToIPF.indexOf("IPF_") + 4, pathToIPF.indexOf("tuple")) + "\t"
                        // no att and their list
                        + pathToIPF.substring(pathToIPF.indexOf("tuples_") + 7, pathToIPF.lastIndexOf(".txt")) + "\t"
                        //cost of the root
                        + tree.getRoot().getCost() + "\t"
                        //time in milliseconds to query (changes from method to method)
                        + queryTime + "\t"
                        // no nodes
                        + tree.getNumberOfNodes() + "\t"
                        //no of relaxation nodes
                        + relaxationNodes + "\t"
                        //level L
                        + level + "\t"
                        //Time to construct the tree
                        + buildingTime + "\t"
                        //Db interrogation time percentage
                        + db.getTotalTimeDbInterrogation() / 1000000 + " = " + nf.format(db.getTotalTimeDbInterrogation() / 10000 / (float) buildingTime) + "%\t"
                        //ipf interrogation time percentage
                        + nf.format(ipfTime / 10000 / (float) buildingTime) + "%" + "\t"
                        //is it a failing query? (at first level) 
                        + failing + "\t"
                        //objective function type
                        + type + "\t"
                        + cardinality + "\t" //cardinality
                        + k + "\t" //k
                        //Opt tree cost
                        + effort + "\t"
                        + answers + "\t"
                        + profit + "\t"
                        + expectedRelaxations + "\t" //Number of expected relaxation 
                        //number of buckets
                        + (typeOfTree == 8 || typeOfTree == 9? buckets : "-1") + "\n"; //total time to interoog the db
                out.append(tmp);

                db.resetTime();//A:
                tree.resetTime();//A:
            }
        } catch (Exception ex) {
            Logger.getLogger(TestFramework.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception ex) {
            }
            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception ex) {
            }
        }
    }

    @CommandInput(
            consoleFormat = "-db",
            defaultValue = "",
            mandatory = true,
            description = "folder containing the database to be used")
    public void setDbFolder(String dbFolder) {
        this.dbFolder = dbFolder;
    }

    @CommandInput(
            consoleFormat = "-l",
            defaultValue = "",
            mandatory = true,
            description = "query file containing the dataset and the query to be performed")
    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    @CommandInput(
            consoleFormat = "-p",
            defaultValue = "IdfFunction",
            mandatory = false,
            description = "the preference function used in the model (choices are [IdfFuction,DatabaseFunction,HammingFunction]) ")
    public void setPreferenceFunction(String preferenceFunction) {
        this.preferenceFunction = preferenceFunction;
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
            consoleFormat = "-L",
            defaultValue = "3",
            mandatory = false,
            description = "level used in the convolution tree")
    public void setLevel(int level) {
        this.level = level;
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
            consoleFormat = "-tt",
            defaultValue = "MIN_EFFORT",
            mandatory = false,
            description = "optimization function used")
    public void setOptimizationCriteria(String optimizationCriteria) {
        this.optimizationCriteria = optimizationCriteria;
    }

    @CommandInput(
            consoleFormat = "-b",
            defaultValue = "10",
            mandatory = false,
            description = "number of buckets used by Convolution tree")
    public void setBuckets(int buckets) {
        this.buckets = buckets;
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
            consoleFormat = "-o",
            defaultValue = "stats.csv",
            mandatory = false,
            description = "output statistics file")
    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    @CommandInput(
            consoleFormat = "-w",
            defaultValue = "false",
            mandatory = false,
            description = "write trees in dot format")    
    public void setWriteTrees(boolean writeTrees) {
        this.writeTrees = writeTrees;
    }

    @CommandInput(
            consoleFormat = "-k",
            defaultValue = "1",
            mandatory = false,
            description = "number of relaxations to return to the user")    
    public void setK(int k) {
        this.k = k;
    }
    
    @CommandInput(
            consoleFormat = "-co", 
            defaultValue = "false", 
            mandatory = false, 
            description = "compare objectives (works only with algorithms described in the paper)")
    public void setCompareObjectives(final boolean compareObjectives) {
        this.compareObjectives = compareObjectives;
    }
    
    @CommandInput(
            consoleFormat = "-bi", 
            defaultValue = "false", 
            mandatory = false, 
            description = "use the biased version of top-k")
    public void setBiased(final boolean biased) {
        this.biased = biased;
    }
}
