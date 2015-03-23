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
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
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
 * @author Davide Mottin, Alice Marascu
 */
public class ConvolutionPruningTree extends ConvolutionTree {

    public ConvolutionPruningTree(Query query, int level) {
        this(query, level, 3, 1, DEFAULT_TYPE);
    }

    public ConvolutionPruningTree(Query query, int levelL, int noOfBuckets, int cardinality, TreeType type) {
        super(query, levelL, noOfBuckets, cardinality, type);
    }


    /*
     * Expand only one each time you iterate. This is easier.
     * We can optimize, if they are all equal do not apply any heuristic.
     */
    @Override
    protected void constructByLevel() throws TreeException {
        RelaxationNode rn;
        ChoiceNode cn;
        LinkedList<Node> queue = new LinkedList<>();
        Node n;
//        bounds.put(currentRoot, new Pair<>(1.0, query.size()));
        queue.add(getCurrentRoot());

        try {
            if (!db.isConnected()) {
                db.connect();
            }
            switch (type) {
                case MAX_VALUE_AVG:
                case MAX_VALUE_MAX:
                    bounds.put(currentRoot, new Pair<>(0.0, db.getMaxBenefit()));
                    break;
                case MIN_EFFORT:
                    bounds.put(currentRoot, new Pair<>(1.0, query.size()));
                    break;
                case PREFERRED:
                    bounds.put(currentRoot, new Pair<>(0.0, pref.compute(query, Utilities.toBooleanQuery(query))));
                    break;
            }
            //Scroll down the tree till the actualLevel
            scrollDown(queue);
            while (!queue.isEmpty() && actualLevel <= this.currentLevel) {
                //Visit only if the node has already been computed
                n = queue.poll();
                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) {
                    if (!marked.contains(n)) { //Do not expand further
                        if (n instanceof RelaxationNode) {
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
                                        bounds.put(cn, new Pair<>(query.size(), query.size()));
                                        if (!marked.contains(n)) {
                                            nodes++;
                                        }
                                    }
                                }
                            } else {
                                //n. internal leaf always
                                // n.setBuckets(bucketize((double)1/noOfBuckets, 1, query.size() - levelL));
                                n.setBuckets(null);//leaf so zero, nothing

                            }
                        } else if (n instanceof ChoiceNode) {
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
                                    //these are not real leavses, so it will be bucketized into [1, N-L] and the mass of each bucket will be 1/noOfBuckets
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
                                if (actualLevel != 1) {
                                    for (Node node : currentRoot.getChildren()) {
                                        if (type == TreeType.MAX_VALUE_AVG || type == TreeType.MAX_VALUE_MAX)
                                            node.setBuckets(bucketize((double) 1 / noOfBuckets, 0, db.getMaxBenefit(), 1));
                                        else 
                                            node.setBuckets(bucketize((double) 1 / noOfBuckets, 1, query.size() - actualLevel, 1));
                                    }
                                }
                                update(queue);
                                prune(queue);
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
                    if ((!optimal || n.cost == child.cost) && !unconsideredBranches.contains(child) && !marked.contains(child)) { //Take the paths with the cost of the root equal to the cost of the parent - i.e. Opt
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
    @Override
    public double optimalPathSimilarity(OptimalRelaxationTree opt) {
        LinkedList<Node> optQueue = new LinkedList<>(), convQueue = new LinkedList<>();
        Node nOpt, nConv;
        optQueue.add(opt.root);
        convQueue.add(root);
        boolean added;
        Map<String, Node> optRel;
        Set<Node> pending = new HashSet<>();
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
                    if (nConv.cost == child.cost && !unconsideredBranches.contains(child) && !marked.contains(child)) { //Take the paths with the cost of the root equal to the cost of the parent
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

//    @Override
//    protected boolean optimalityCondition(Node n1, Node n2) {
//        return n1.cost == n2.cost && !unconsideredBranches.contains(n2) && !marked.contains(n2);
//    }
    
    @Override
    protected RelaxationTree optimalTree(RelaxationTree optTree) throws TreeException {
        ConvolutionTree t = new ConvolutionPruningTree(query, level, noOfBuckets, cardinality, type);
        t.marked = new HashSet<>();
        t.unconsideredBranches = new HashSet<>();
        return super.optimalTree(t);
    }
    
    @Override
    protected Node getCurrentRoot() {
        return currentRoot; 
    }

}
