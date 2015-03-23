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
package it.unitn.disi.db.queryrelaxation.tree;

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Compute the CDR algorithm realizing the first L level of the tree and then 
 * choosing the branch which maximizes the probaility tha the cost is smaller
 * than all the other siblings. This methods approximates the distribution of
 * the costs for each node and uses convolution to compute minimum and sum of
 * the distributions.
 * @author Alice Marascu
 */
public class ConvolutionTree extends PruningTree {
    /*
     * Branches not expanded from the convolution methods, probabilistically worse
    */
    protected Set<Node> unconsideredBranches;
    /*
     * Current root being considered to the expansion. 
    */
    protected Node currentRoot;
    protected int numberOfSteps; //To keep the number of steps and average the time. 
    protected int level; //A: fixed value specified in the output
    protected int currentLevel;
    protected int noOfBuckets; //A: added the number of buckets for prob distr
    protected int indexAddendum; 
    
    public ConvolutionTree(Query query, int level) {
        this(query, level, 3, 1, DEFAULT_TYPE);
    }

    public ConvolutionTree(Query query, int levelL, int noOfBuckets, int cardinality, TreeType type) {
        super(query, cardinality, type);
        this.currentRoot = root;
        this.level = levelL;
        this.noOfBuckets = noOfBuckets;
        this.indexAddendum = type.isMaximize()? 0 : 1;
    }

    @Override
    public void buildIteratively() throws TreeException {
        unconsideredBranches = new HashSet<>();
//        computedProbabilities = new HashMap<Query, Double>();
        numberOfSteps = 0;
        marked = new HashSet<>();
        bounds = new HashMap<>();
        LinkedList<Node> roots = new LinkedList<>();
        Map<Node, Integer> uncompletedBranches = new HashMap<>();
        Node lastRoot, n;
        currentLevel = level;
        roots.add(root);
        nodes++;
        uncompletedBranches.put(root, 1);
        while (!roots.isEmpty()) {
            lastRoot = currentRoot = roots.poll();
            actualLevel = uncompletedBranches.remove(currentRoot);
            if (actualLevel > currentLevel) {
                currentLevel++;
            }
            constructByLevel();
            numberOfSteps++;
            if (currentLevel < query.size() && query.size() > level) {
                computeApproximation();
                if (currentRoot != lastRoot) { //Empty node or end of the tree
                    for (int i = 0; i < currentRoot.getChildren().size(); i++) {
                        n = currentRoot.getChildren().get(i);
                        if (!n.isLeaf()) {
                            roots.add(n);
                            uncompletedBranches.put(n, (int) actualLevel);
                        }
                    }
                }
            }
            //Increment only if needed. 
        }
    }


    /*
     * Expand only one each time you iterate. This is easier.
     * We can optimize, if they are all equal do not apply any heuristic.
     */
    protected void constructByLevel() throws TreeException {
        RelaxationNode rn;
        ChoiceNode cn;
        //long currentTime = 0;
        LinkedList<Node> queue = new LinkedList<>();
        Node n;
        Query q;
//        marked  = new HashSet<>();
//        time = 0;
//        bounds.put(currentRoot, new Pair<>(1.0, query.size()));
        queue.add(currentRoot);

        try {
            if (!db.isConnected()) {
                db.connect();
            }
            //Scroll down the tree till the actualLevel where you want to start the expansion
            scrollDown(queue);
            while (!queue.isEmpty() && actualLevel <= this.currentLevel) {
                //Visit only if the node has already been computed
                n = queue.remove(0);
                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) {
                    if (!marked.contains(n)) {
                        //currentTime = System.nanoTime();

                        if (n instanceof RelaxationNode) {
                            //1. hard constr stop version
//                          //DAVIDE-MOD 14/07/2014 - Check constructRelaxationNodes condition
                            if (((RelaxationNode) n).isEmpty()) {
                                for (Constraint c : n.getQuery().getConstraints()) {
                                    if (!c.isHard()) {
                                        cn = new ChoiceNode();
                                        cn.setFather(n);
                                        cn.setConstraint(c);
                                        cn.setQuery((Query) n.getQuery().clone());
                                        ((RelaxationNode) n).addNode(c.getAttributeName(), cn);
                                        queue.add(cn);
                                        // Put first upper bounds and lower bounds
                                        //bounds.put(cn, new Pair<Double, Double>(actualLevel, query.size()));
                                        if (!marked.contains(n)) {
                                            nodes++;
                                        }
                                    }
                                }
                            } else { 
                                // n. internal leaf always
                                // n. setBuckets(bucketize((double)1/noOfBuckets, 1, query.size() - levelL));
                                n.setBuckets(null);//leaf so zero, nothing
                            }
                        } 
                        else if (n instanceof ChoiceNode) {
                            //Build 'yes' node
                            cn = (ChoiceNode) n;
                            queue.add(constructRelaxationNodes(cn, true));
                            //Build 'no' node
                            rn = constructRelaxationNodes(cn, false);
                            cn.setNoNode(1 - cn.getYesProbability(), rn);
                            //cn.setNoNode(computeNoProbabilitySecondVersion((Query) n.getQuery().clone(), (RelaxationNode) n.father), rn);
                            queue.add(rn);
                            if (!marked.contains(n)) {
                                nodes += 2;
                                relaxationNodes += 2;
                            }
                            if (actualLevel + 1 == currentLevel) {//n is a leaf level L CHECK
                                if (query.size() - level == 0) {
                                    cn.getNoNode().setBuckets(null);  //TODO?? to ignore, so put zero
                                    cn.getYesNode().setBuckets(null);  //TODO??
                                } else if (query.size() - level == 1) {
                                    //  cn.getNoNode().setBuckets(bucketize((double) 1 / noOfBuckets, MIN_LEAF_BUCKET_LIMIT, MAX_LEAF_BUCKET_LIMIT)); //TODO??  to add as number
                                    //  cn.getYesNode().setBuckets(bucketize((double) 1 / noOfBuckets, MIN_LEAF_BUCKET_LIMIT, MAX_LEAF_BUCKET_LIMIT)); //TODO??
                                    cn.getNoNode().setBuckets(bucketize((double) 1, -1, -1, 0)); //TODO??  to add as number
                                    cn.getYesNode().setBuckets(bucketize((double) 1, -1, -1, 0)); //TODO??
                                } else {
                                    //these are not real leavses, so it will be bucketized into [1, N-L] and the mass of each bucket will be 1/noOfBucket
                                    ///TODO: -- cn.getNoNode().setBuckets(bucketize((double) 1 / noOfBuckets, 0, max [], 1));
                                    if (type == TreeType.MAX_VALUE_AVG || type == TreeType.MAX_VALUE_MAX) {
                                        cn.getNoNode().setBuckets(bucketize((double) 1 / noOfBuckets, 0, db.getMaxBenefit(), 1));
                                        cn.getYesNode().setBuckets(bucketize((double) 1 / noOfBuckets, 0, db.getMaxBenefit(), 1));
                                    } else {
                                        cn.getNoNode().setBuckets(bucketize((double) 1 / noOfBuckets, 1, query.size() - level, 1));
                                        cn.getYesNode().setBuckets(bucketize((double) 1 / noOfBuckets, 1, query.size() - level, 1));
                                    }

                                }

                            }
                            //End of the level - Top of the queue is a relaxation node
                            if (queue.get(0) instanceof RelaxationNode) {
                                //Update and prune
                                /*
                                 * FILL for the first time the uncompleted branches map,
                                 * We use it to understand which are complete and which not
                                 */
//                                if (uncompletedBranches.isEmpty()) {
//                                    for (Node node : currentRoot.getChildren()) {
//                                        uncompletedBranches.put(node, (int) actualLevel);
//                                    }
//                                }
//                                if (consideredBranch != null) {
//                                    uncompletedBranches.put(consideredBranch, (int) actualLevel);
                                if (actualLevel != 1) {
                                    for (Node node : currentRoot.getChildren()) {
                                        //uncompletedBranches.put(node, (int) actualLevel);
                                        
                                        ///TODO: -- cn.getNoNode().setBuckets(bucketize((double) 1 / noOfBuckets, 0, max [], 1));
                                        if (type == TreeType.MAX_VALUE_AVG || type == TreeType.MAX_VALUE_MAX)
                                            node.setBuckets(bucketize((double) 1 / noOfBuckets, 0, db.getMaxBenefit(), 1));
                                        else 
                                            node.setBuckets(bucketize((double) 1 / noOfBuckets, 1, query.size() - actualLevel, 1));
                                    }
                                }
                                //update(queue);
                                //prune(queue);
                                if (verbose) {
                                    System.out.println(toString());
                                }
                                ++actualLevel;
                            }
                        }
                    }

                } //END IF NOT EMPTY QUERY
            }
            //System.out.println("A: L=" + actualLevel); //A:
        } catch (Exception ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        }
    }

    
    
    /*
     * Since you have choosen a new root go down till the level to be expanded
     */
    protected void scrollDown(LinkedList<Node> queue) {
        Node n;
        int i = (currentLevel - level) + 1;
        while (!queue.isEmpty() && i < actualLevel) {
            n = queue.poll();
            if (!n.getChildren().isEmpty()) {
                queue.addAll(n.getChildren());
            }
            if (n instanceof ChoiceNode && queue.peek() instanceof RelaxationNode) {
                i++;
            }
        }
    }

    /**
     * This computes the approximated cost of a node, depending on the cost of the subtrees
     * @param n The node to update the cost.
     * @throws it.unitn.disi.db.queryrelaxation.tree.TreeException
     * @see ChoiceNode
     * @see RelaxationNode
     */
    public void updateApproximation(Node n) throws TreeException {

        double[][] distr1PlusC1, distr1PlusC2, distrMPYes, distrMPNo, finalSum = null;
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());


            /*  //To check this
            if(cn.getYesNode().getBuckets() == null){
            cn.getYesNode().setBuckets( bucketize((double)1/noOfBuckets, 1, (double)cn.getYesNode().getLevel()));
            }
            if(cn.getNoNode().getBuckets() == null){
            cn.getNoNode().setBuckets( bucketize((double)1/noOfBuckets, 1, (double)cn.getYesNode().getLevel()));
            }*/

            if (cn.getYesNode().getBuckets() == null && cn.getNoNode().getBuckets() == null) {
                //leaf on the level L which is also the last one: put zero to the relaxation father   //AA:
                //or leaf where yes gives answer and no will never give answer so full stop
                // finalSum = bucketize((cn.getYesProbability() + cn.getNoProbability()) / noOfBuckets, MIN_LEAF_BUCKET_LIMIT, MAX_LEAF_BUCKET_LIMIT); // TODO: interval???
                finalSum = bucketize((cn.getYesProbability() + cn.getNoProbability()) / noOfBuckets, -1, -1, 0); // TODO: interval???

            } else if (cn.getNoNode().getBuckets() == null) {//the yes is null
                distr1PlusC1 = combineDistr(0, cn.getYesNode().getBuckets(),
                        //bucketize((double) 1 / noOfBuckets, cn.getYesNode().getBuckets()[0][0], cn.getYesNode().getBuckets()[0][noOfBuckets]));
                        bucketize(c, -1, -1, 0));
                // System.out.println("1");
                distrMPYes = combineDistr(1, distr1PlusC1,
                        //  bucketize(cn.getYesProbability() / noOfBuckets, distr1PlusC1[0][0], distr1PlusC1[0][noOfBuckets]));
                        bucketize(cn.getYesProbability(), -1, -1, 0));

                finalSum = combineDistr(0,
                        // bucketize(cn.getNoProbability() / noOfBuckets, distrMPYes[0][0], distrMPYes[0][noOfBuckets]), distrMPYes);
                        bucketize(cn.getNoProbability(), -1, -1, 0), distrMPYes);


            } else if (cn.getYesNode().getBuckets() == null) { //so it's not a leaf, so cost is not zero
                //System.out.println("2");
                distr1PlusC2 = combineDistr(0, cn.getNoNode().getBuckets(),
                        // bucketize((double) 1 / noOfBuckets, cn.getNoNode().getBuckets()[0][0], cn.getNoNode().getBuckets()[0][noOfBuckets]));
                        bucketize(c, -1, -1, 0));
                distrMPNo = combineDistr(1, distr1PlusC2,
                        //bucketize(cn.getNoProbability() / noOfBuckets, distr1PlusC2[0][0], distr1PlusC2[0][noOfBuckets]));
                        bucketize(cn.getNoProbability(), -1, -1, 0));

                finalSum = combineDistr(0,
                        // bucketize(cn.getYesProbability() / noOfBuckets, distrMPNo[0][0], distrMPNo[0][noOfBuckets]), distrMPNo);
                        bucketize(cn.getYesProbability(), -1, -1, 0), distrMPNo);

            } else if (cn.getYesNode().getBuckets()[0][noOfBuckets] == 1 && cn.getNoNode().getBuckets()[0][noOfBuckets] == 1) {
                //there is just one value
                //finalSum = bucketize((cn.getYesProbability() * 2 + cn.getNoProbability() * 2) / noOfBuckets, MIN_LEAF_BUCKET_LIMIT, MAX_LEAF_BUCKET_LIMIT);
                finalSum = bucketize((cn.getYesProbability() * 2 + cn.getNoProbability() * 2), -1, -1, 0);


            } else {
                distr1PlusC1 = combineDistr(0, cn.getYesNode().getBuckets(),
                        //bucketize(1 / noOfBuckets, cn.getYesNode().getBuckets()[0][0], cn.getYesNode().getBuckets()[0][noOfBuckets]));
                        bucketize((int)c, -1, -1, 0));
                //    System.out.println("3"+display(distr1PlusC1)+ "\n result of:"+display(cn.getYesNode().getBuckets())+ "\n and:"+ display( bucketize(1, -1, -1, 0)));
                distrMPYes = combineDistr(1, distr1PlusC1,
                        // bucketize(cn.getYesProbability() / noOfBuckets, distr1PlusC1[0][0], distr1PlusC1[0][noOfBuckets]));
                        bucketize(cn.getYesProbability(), -1, -1, 0));

                distr1PlusC2 = combineDistr(0, cn.getNoNode().getBuckets(),
                        //bucketize(1 / noOfBuckets, cn.getNoNode().getBuckets()[0][0], cn.getNoNode().getBuckets()[0][noOfBuckets]));
                        bucketize((int)c, -1, -1, 0));
                //  System.out.println("4");
                distrMPNo = combineDistr(1, distr1PlusC2,
                        //bucketize(cn.getNoProbability() / noOfBuckets, distr1PlusC2[0][0], distr1PlusC2[0][noOfBuckets]));
                        bucketize(cn.getNoProbability(), -1, -1, 0));


                finalSum = combineDistr(0, distrMPYes, distrMPNo);
            }
            cn.setBuckets(finalSum);

        } else if (n instanceof RelaxationNode) {
//            double minProb = -1, tmpProb, tmp;
            //Root computation
            if (n == currentRoot) {
                computeTopRoots();
            } else {
                double min = Double.MAX_VALUE;
                for (Node child : n.getChildren()) {
                    if (!marked.contains(child)
                            && child.getCost() < min) {
                        min = child.getCost();
                    }
                }
                n.setCost(min);
                //convolution min/max
                double[][] tmpDiff;
                tmpDiff = n.getChildren().get(0).getBuckets();
                for (int i = 1; i < n.getChildren().size(); i++) {
                    if (!marked.contains(n.getChildren().get(i))) {
                        tmpDiff = type.isMaximize()?
                            convolutionMax(tmpDiff, n.getChildren().get(i).getBuckets()) : 
                            convolutionMin(tmpDiff, n.getChildren().get(i).getBuckets());
                    }
                }
                n.setBuckets(tmpDiff);
            }
        }
    }

    protected void computeTopRoots() {
        double maxProb = Double.MAX_VALUE, tmpProb, tmp; //1.
        Node bestRootChild = currentRoot; //can be erased

        for (Node child1 : currentRoot.getChildren()) {
            if (!marked.contains(child1)) {
                if (verbose) {//Very verbose
                    System.out.println(Utilities.matrixToString(child1.buckets));
                }
                tmpProb = 0;
                //   checkIfBucketsAreAProb(child1.getBuckets());
                for (Node child2 : currentRoot.getChildren()) {
                    //  checkIfBucketsAreAProb(child2.getBuckets());
                    if (child1 != child2) {
                        //compute p(child1 < child2)
                        for (int i = 0; i < noOfBuckets - 1; i++) {
                            tmp = 0;
                            for (int j = i + indexAddendum; j < noOfBuckets - 1; j++) {
                                tmp += child2.getBuckets()[1][j];
                            }
                            tmpProb += tmp * child1.getBuckets()[1][i];
                        }
                    }
                }
                if (verbose)
                    System.out.printf("Prob: %f - Child: %s - %s\n", tmpProb, child1.toString(), ((ChoiceNode) child1).getYesNode().query);

                if (type.isMaximize()) {
                    tmpProb = 1 - tmpProb;
                }

                if (maxProb < tmpProb || maxProb == Double.MAX_VALUE) { //
                    maxProb = tmpProb;
                    bestRootChild = child1;//can be erase
                }
            }
        }
        if (verbose) {
            System.out.printf("Best root child: %s", ((ChoiceNode) bestRootChild).getYesNode().query);
            //System.out.println("Convolution-Cost of the root is:" + minProb + " for child:" + bestRootChild);
            System.out.println("Convolution-Cost of the root is:" + maxProb + " for child:" + bestRootChild);
        }
        unconsideredBranches.addAll(bestRootChild.getSiblings());
        currentRoot = bestRootChild;
    }
    
    
    /**
     * Computes the approximation that chooses the best branch among the list of
     * possible relaxations.
     * @throws it.unitn.disi.db.queryrelaxation.tree.TreeException
     */
    protected void computeApproximation() throws TreeException {  //A: convolution adapted
        LinkedList<Node> stack = new LinkedList<>();
        LinkedList<Integer> currentChild = new LinkedList<>();
        Node currentNode, child;
        Integer actualChild;

        stack.push(root);
        currentChild.push(0);
        try {
            while (!stack.isEmpty()) {
                currentNode = stack.peek();
                actualChild = currentChild.pop();
                if (currentNode.isLeaf()) {
                    stack.pop();
                    if (!(currentNode instanceof RelaxationNode)) {
                        throw new TreeException("Wrong leaf node type, it should be a RelaxationNode");
                    }
                    currentNode.setCost(0);
                } else if (currentNode.getChildren().size() <= actualChild) {//Visited all the childs
                    stack.pop();
                    updateApproximation(currentNode);
                } else {
                    child = currentNode.getChildren().get(actualChild);
                    currentChild.push(actualChild + 1);
                    stack.push(child);
                    currentChild.push(0);
                }
            }
        } catch (Exception ex) {
            throw new TreeException("Error on cost computation", ex);
        }
    }

    /**
     * Computes the min among PDFs using convolution
     * @param buckets1 The first bucket to combine
     * @param buckets2 The second bucket to combine
     * @return The convoluted PDF (min operator)
     */
    public double[][] convolutionMin(double[][] buckets1, double[][] buckets2) {
        //  double maxValue = Math.max(buckets1[0][noOfBuckets], buckets2[0][noOfBuckets]),
        //         minValue = Math.min(buckets1[0][0], buckets2[0][0]);
        double[][] minBuckets = bucketize(0, Math.min(buckets1[0][0], buckets2[0][0]),
                Math.max(buckets1[0][noOfBuckets], buckets2[0][noOfBuckets]), 1);
        double tmp1, tmp2;
        for (int i = 0; i < noOfBuckets - 1 /*+ 1 - 1*/; i++) {
            tmp1 = 0;
            tmp2 = 0;
            for (int j = i + 1; j < noOfBuckets /*+ 1 - 1*/; j++) {
                tmp1 += buckets1[1][i] * buckets2[1][j];
                tmp2 += buckets2[1][i] * buckets1[1][j];
            }
            minBuckets[1][i] = buckets1[1][i] * buckets2[1][i] + tmp1 + tmp2;
        }

//        if (!areBucketsPdfs(buckets1) || !areBucketsPdfs(buckets2) || !areBucketsPdfs(minBuckets)) {
//            System.err.println("Buckets:" + display(buckets1) + " MIN " + display(buckets2) + " = " + display(minBuckets));
//        }
        // System.out.println("MinConv of:"+display(buckets1)+" MIN "+ display(buckets2)+" = "+display(minBuckets));
        return minBuckets;
    }

    /**
     * Computes the min among PDFs using convolution
     * @param buckets1 The first bucket to combine
     * @param buckets2 The second bucket to combine
     * @return The convoluted PDF (min operator)
     */
    public double[][] convolutionMax(double[][] buckets1, double[][] buckets2) {
        //  double maxValue = Math.max(buckets1[0][noOfBuckets], buckets2[0][noOfBuckets]),
        //         minValue = Math.min(buckets1[0][0], buckets2[0][0]);        
        double[][] maxBuckets = bucketize(0, Math.max(buckets1[0][0], buckets2[0][0]),
                Math.max(buckets1[0][noOfBuckets], buckets2[0][noOfBuckets]), 1);
        double tmp1, tmp2;
        for (int i = 0; i < noOfBuckets - 1 /*+ 1 - 1*/; i++) {
            tmp1 = 0;
            tmp2 = 0;
            for (int j = 0; j <= i /*+ 1 - 1*/; j++) {
                tmp1 += buckets1[1][i] * buckets2[1][j];
                tmp2 += buckets2[1][i] * buckets1[1][j];
            }
            maxBuckets[1][i] = tmp1 + tmp2;
        }

//        if (!areBucketsPdfs(buckets1) || !areBucketsPdfs(buckets2) || !areBucketsPdfs(maxBuckets)) {
//            System.err.println("Buckets:" + display(buckets1) + " MAX " + display(buckets2) + " = " + display(maxBuckets));
//        }
        // System.out.println("MinConv of:"+display(buckets1)+" MIN "+ display(buckets2)+" = "+display(minBuckets));
        return maxBuckets;
    }
    
    
    
    
    /**
     * Compute the combination of PDFs (buckets) using the convolution method
     * @param typeCombination 0-sum, 1-product  
     * @param buckets1 The first bucket to combine
     * @param buckets2 The second bucket to combine
     * @return The convoluted bucket
     */
    protected double[][] combineDistr(int typeCombination, double[][] buckets1, double[][] buckets2) {
        double[][] resultBuckets = bucketize(0, buckets1[0][0] + buckets2[0][0], buckets1[0][noOfBuckets] + buckets2[0][noOfBuckets], 1);

        /*  double[][] resultBuckets =  new double[2][noOfBuckets+1];
        double maxValue = buckets1[0][noOfBuckets] + buckets2[0][noOfBuckets],
        minValue = buckets1[0][0] + buckets2[0][0];
        
        //initialize buckets limits
        for(int i= 0; i < resultBuckets.length; i++){
        resultBuckets[0][i] = (maxValue - minValue) / (double)noOfBuckets * (i+1);
        }
         */
        //commpute prob1 (* or +) prob2

        switch (typeCombination) {
            case 0: //plus
                for (int i = 0; i < noOfBuckets /*+ 1 - 1*/; i++) {
                    for (int j = 0; j < noOfBuckets /*+ 1 - 1*/; j++) {
                        //resultBuckets[1][(i + j) / 2] += (buckets1[1][i] + buckets2[1][j]) / noOfBuckets;
                        resultBuckets[1][(i + j) / 2] += (buckets1[1][i] + buckets2[1][j]) / noOfBuckets / 2;
                    }
                }
//                if (checkIfBucketsAreAProb(buckets1) == false || checkIfBucketsAreAProb(buckets2) == false) {
                    //  System.out.println("SUM: Buckets:"+ display(buckets1)+" PLUS "+ display(buckets2)+" = "+display(resultBuckets));
//                }
                // System.out.println("Buckets:"+ display(buckets1)+" PLUS "+ display(buckets2)+" = "+display(resultBuckets));
                break;
            case 1: //multiplication
                for (int i = 0; i < noOfBuckets /*+ 1 - 1*/; i++) {
                    for (int j = 0; j < noOfBuckets /*+ 1 - 1*/; j++) {
                        resultBuckets[1][(i + j) / 2] += (buckets1[1][i] * buckets2[1][j]) / noOfBuckets;
                    }
                }
//                if (checkIfBucketsAreAProb(buckets1) == false || checkIfBucketsAreAProb(buckets2) == false) {
                    // System.out.println("MULTIPLICATION: Buckets:"+ display(buckets1)+" X "+ display(buckets2)+" = "+display(resultBuckets));
//                }
                //  System.out.println("Buckets:"+ display(buckets1)+" x "+display(buckets2)+" = "+display(resultBuckets));
                break;
            default:
                System.out.println("Probability distribution unknown combination");
                break;
        }

        return resultBuckets;
    }

    /**
     * Bucketize a value in noOfBuckets buckets from minValue to maxValue and the mass
     * of each bucket is = 1 if the value is in the range of that bucket else zero
     * 
     * @param value The value  to put in each bucket
     * @param minValue The min value of the random variable
     * @param maxValue Tha max value of the random variable
     * @param type type = 0 if there is a constant, type = 1 if the value must 
     * be put in each bucket (these are not real leavses, so it will be bucketized into [1, N-L] and the mass of each bucket will be 1/noOfBuckets
     * type = 2 puts the value = value in all buckets
     * @return 
     */
    protected double[][] bucketize(double value, double minValue, double maxValue, int type) { //A: added
        double[][] buckets = new double[2][noOfBuckets + 1];
        
        switch (type) {
            case 0: //constant bucket-ization
                //it will compare with the right limit, so (], except if value is zero, where [)
                if (value < 1) {//System.out.println("Value "+ value+" <1 to be bucketized as constant)");
                    minValue = 0;
                    maxValue = 1;
                    double tmp = (maxValue - minValue) / (double) noOfBuckets;

                    if (value != 0) {
                        for (int i = 0; i < noOfBuckets + 1; i++) {
                            buckets[0][i] = minValue + tmp * i; //bucket's limits
                            if (value > buckets[0][i] && value <= buckets[0][i] + tmp) {
                                buckets[1][i] = 1;  //bucket containg the value
                            } else {
                                buckets[1][i] = 0;  //zero
                            }
                        }
                    } else {//value is zero, and it compares to the left limit of the bucket
                        for (int i = 0; i < noOfBuckets + 1; i++) {
                            buckets[0][i] = minValue + tmp * i; //bucket's limits
                            if (value >= buckets[0][i] && value < buckets[0][i] + tmp) {
                                buckets[1][i] = 1;  //bucket containg the value
                            } else {
                                buckets[1][i] = 0;  //zero
                            }
                        }
                    }

                    buckets[1][noOfBuckets] = -1;
                } else { /* so   value >=1*/
                    minValue = value - 1;
                    maxValue = value;
                    double tmp = (maxValue - minValue) / (double) noOfBuckets;

                    for (int i = 0; i < noOfBuckets + 1; i++) {
                        buckets[0][i] = minValue + tmp * i; //bucket's limits
                        if (value > buckets[0][i] && value <= buckets[0][i] + tmp) {
                            buckets[1][i] = 1;  //bucket containg the value
                        } else {
                            buckets[1][i] = 0;  //zero
                        }
                    }

                    buckets[1][noOfBuckets] = -1;
                }
                break;
            case 1: 
                //if the value must be put in each bucket (these are not real leavses, so
//              it will be bucketized into [1, N-L] and the mass of each bucket will be 1/noOfBuckets
                //the step size is computed with tmp, and this gives you the single intervals
                //in the histogram. 
                double tmp = (maxValue - minValue) / (double) noOfBuckets;

                for (int i = 0; i < noOfBuckets + 1; i++) {
                    buckets[0][i] = minValue + tmp * i; //bucket's limits - steps
                    buckets[1][i] = value;  //bucket's value
                }

                buckets[1][noOfBuckets] = -1;   //last bucket is not used
                break;
//            case 2: { //puts the value = value in all buckets
//                double tmp = (maxValue - minValue) / (double) noOfBuckets;
//
//                for (int i = 0; i < noOfBuckets + 1; i++) {
//                    buckets[0][i] = minValue + tmp * i; //bucket's limits
//                    buckets[1][i] = value;  //bucket's value
//                }
//
//                buckets[1][noOfBuckets] = -1;   //last bucket is not used
//            }
//            break;
            default:
                System.out.println("unknown type");
                break;
        }

        //System.out.println(" Min/Max = "+minValue+"/"+maxValue+" with value="+value+" was bucketized to "+display(buckets));
        return buckets;
    }

//bucketize a value in noOfBuckets buckets from minValue to maxValue and the mass
//of each bucket is = value
// can be used for initialization nodes of level L with value (double)1/noOfBuckets
    /*
    @Deprecated
    private double[][] bucketizeOLD(double value, double minValue, double maxValue) { //A: added

        double[][] buckets = new double[2][noOfBuckets + 1];
        double tmp = (maxValue - minValue) / (double) noOfBuckets;

        for (int i = 0; i < noOfBuckets + 1; i++) {
            buckets[0][i] = minValue + tmp * i; //bucket's limits
            buckets[1][i] = value;  //bucket's value
        }

        buckets[1][noOfBuckets] = -1;   //last bucket is not used

        //  System.out.println(" Min/Max = "+minValue+"/"+maxValue+" with value="+value+" was bucketized to "+display(buckets));
        return buckets;
    }
     */

    /**
     * This computes the cost of a node, depending on the cost of the subtrees
     * if the node is a <code>ChoiceNode</code> the cost is (c(yes) + c)*p_yes +
     * (c(no) + c)*p_no, if it is a <code>RelaxationNode</code> the cost is
     * max (c[q']), where q' is a direct subquery of q. 
     * @param n The node to update the cost.
     * @see ChoiceNode
     * @see RelaxationNode
     */
    @Override
    public void updateCost(Node n) throws TreeException {

        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());
        } else if (n instanceof RelaxationNode) {
            double min = Double.MAX_VALUE;
            double max = -(Double.MAX_VALUE);
            for (Node child : n.getChildren()) {
                if (!unconsideredBranches.contains(child) && !marked.contains(child)) {
                    if (child.getCost() < min) 
                        min = child.getCost();
                    if (child.getCost() > max)
                        max = child.getCost();
                }
            }
            n.setCost(type.isMaximize()? max : min);
        }
    }

//    public double getExpectedRootCost() {
//        double mean = 0;
//        //root.setCost(min);
//        //convolution min
//        double[][] tmpDiff;
//        tmpDiff = root.getChildren().get(0).getBuckets();
//        for (int i = 1; i < root.getChildren().size(); i++) {
//            if (!marked.contains(root.getChildren().get(i))) {
//                tmpDiff = convolutionMin(tmpDiff, root.getChildren().get(i).getBuckets());
//            }
//        }
//        //root.setBuckets(tmpDiff);
//
//        for (int i = 0; i < noOfBuckets; i++) {
//            System.out.printf("(%f,%f)\n", tmpDiff[0][i], tmpDiff[1][i]);
//            mean += tmpDiff[0][i] * tmpDiff[1][i];
//        }
//        return mean;
//    }

    public String display(double[][] buckets) {
        String tmp = "";
        for (int i = 0; i < noOfBuckets + 1; i++) {
            tmp += "[" + buckets[0][i] + "/" + buckets[1][i] + "] ";
        }
        return tmp;
    }

    public int getLevel() {
        return level;
    }
    
    /**
     * Retreives the time to propose the next single relaxation. 
     * @return Time to query in milliseconds
     */
    @Override
    public long getTime() {
        return time.getElapsedTimeMillis() / numberOfSteps;
    }

    public int getNumberOfSteps() {
        return numberOfSteps;
    }

    public boolean areBucketsPdfs(double[][] buckets) { // check is sum of the buckets is 1
        float tmp = 0;

        for (int i = 0; i < noOfBuckets + 1; i++) {
            tmp += buckets[1][i];
        }
//        if (tmp > 1) {
//            System.out.println("Sum of values is not 1 (" + tmp + ") for these buckets:");
//            System.out.println(display(buckets));
//
//            return false;
//        }
//        return true;
        return tmp == 1;
    }

    /**
     * Print the paths starting from the input node and saving them into a string
     * used as an accumulator. 
     * @param n The starting node
     * @param acc The string to save the paths
     * @param optimal True if we need only optimal paths, false otherwise
     * @return The paths separated by a \n
     */
    @Override
    protected String printPaths(Node n, String acc, boolean optimal) {
        if (n instanceof RelaxationNode) {
            if (n.isLeaf()) {
                return acc + "\n";
            } else {
                String result = "";
                for (Node child : n.getChildren()) {
                    if ((!optimal || n.cost == child.cost) && !unconsideredBranches.contains(child)) { //Take the paths with the cost of the root equal to the cost of the parent - i.e. Opt
                        result += printPaths(child, acc, optimal);
                    }
                }
                return result;
            }
        } else {
            //ChoiceNode cannot be a leaf
            ChoiceNode cn = (ChoiceNode) n;
//            return printPaths(cn.getYesNode(), acc + cn.getConstraint().getAttributeName() + "|" + cn.getYesProbability() +  "|", optimal) +
//            printPaths(cn.getNoNode(), acc + cn.getConstraint().getAttributeName() + "|" + cn.getNoProbability() + "|", optimal);
            return printPaths(cn.getYesNode(), acc + cn.getConstraint().getAttributeName() + "|" + "yes" + "|", optimal)
                    + printPaths(cn.getNoNode(), acc + cn.getConstraint().getAttributeName() + "|" + "no" + "|", optimal);
        }
    }

    /**
     * Compute the (maximum) path similarity between this tree and the optimal. 
     * Print the longest common subsequence among the paths. 
     * @param opt The optimal tree
     * @return The path similarity value
     */
    public double optimalPathSimilarity(OptimalRelaxationTree opt) {
        LinkedList<Node> optQueue = new LinkedList<Node>(), convQueue = new LinkedList<Node>();
        Node nOpt, nConv;
        optQueue.add(opt.root);
        convQueue.add(root);
        boolean added;
        Map<String, Node> optRel;
        Set<Node> pending = new HashSet<Node>();
        String attName;
        int maxConvHeight = 0, maxOptHeight = 1;
        while (!optQueue.isEmpty()) {
            pending.clear();
            nConv = convQueue.poll();
            nOpt = optQueue.poll();
            if (nConv instanceof RelaxationNode) {
                optRel = candidateOptimalRelaxations(nOpt);
                added = false;
                for (Node child : nConv.getChildren()) {
                    if (nConv.cost == child.cost && !unconsideredBranches.contains(child)) { //Take the paths with the cost of the root equal to the cost of the parent
                        attName = ((ChoiceNode) child).getConstraint().getAttributeName();
                        if (optRel.containsKey(attName)) {
                            if (maxConvHeight < child.getLevel() + 1) {
                                maxOptHeight = maxConvHeight = child.getLevel() + 1;
                            }
                            optQueue.add(optRel.get(attName));
                            convQueue.add(child);
                            added = true;
                        }
                    }
                }
                if (!added) {//If nothing has been added we need to add nodes to pending
                    pending.addAll(optRel.values());
                }
            } else if (nConv instanceof ChoiceNode) {
                optQueue.addAll(nOpt.getChildren());
                convQueue.addAll(nConv.getChildren());
            }
        }
        if (!pending.isEmpty()) { //compute max opt hieght
            optQueue.addAll(pending);
            while (!optQueue.isEmpty()) {
                nOpt = optQueue.poll();
                if (nOpt instanceof RelaxationNode) {
                    for (Node child : nOpt.getChildren()) {
                        if (child.cost == nOpt.cost) {
                            optQueue.add(child);
                            if (maxOptHeight < child.getLevel() + 1) {
                                maxOptHeight = child.getLevel() + 1;
                            }
                        }
                    }
                } else {
                    optQueue.addAll(nOpt.getChildren());
                }
            }
        } else {
            maxOptHeight = maxConvHeight; //So the similarity is 1
        }
        return maxConvHeight / (double) maxOptHeight;
    }

    /*
     * Compute candidate optimal relaxations to be used in the computation of 
     * the paths. 
     */
    protected Map<String, Node> candidateOptimalRelaxations(Node rn) {
        Map<String, Node> optRel = new HashMap<String, Node>();
        for (Node n : rn.getChildren()) {
            if (rn.cost == n.cost) {
                optRel.put(((ChoiceNode) n).getConstraint().getAttributeName(), n);
            }
        }
        return optRel;
    }
    
    @Override
    protected boolean optimalityCondition(Node n1, Node n2) {
        return n1.cost == n2.cost && !unconsideredBranches.contains(n2) && !marked.contains(n2);
    }

    @Override
    protected boolean isMarked(Node n) {
        return marked.contains(n) || unconsideredBranches.contains(n);
    }
    
    

    @Override
    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
        ConvolutionTree t = new ConvolutionTree(query, level, noOfBuckets, cardinality, tt);
        t.marked = new HashSet<>();
        t.unconsideredBranches = new HashSet<>();
        return super.optimalTree(t);
    }

    @Override
    protected Node getCurrentRoot() {
        return currentRoot;
    }
    
    
}
