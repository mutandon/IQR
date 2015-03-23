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
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Build a <code>PruningTree</code> using some heuristic method, like explore
 * the minimum upper bound branch first.
 *
 * MODIFICATIONS: 01/28/2012 - Leaves for no nodes scrathed up everything
 * 01/25/2013 - Generalized the cost function, changed the bounds computation
 *
 * @see PruningTree
 * @author Davide Mottin
 */
public class HeuristicPruningTree extends PruningTree {
    /*
     * Waiting list of nodes to be considered
     */

    //private LinkedList<Node> waiting;
    /*
     * Branches of the tree not yet expanded
     */
    private Map<Node, Integer> uncompletedBranches;
    /*
     * Branch considered in the expansion of the tree
     */
    private Node consideredBranch;
    /*
     * Non empty if the considered branch is expandable
     */
    private Set<Node> expandableNodes;

    /**
     * Enumeration to choose the strategy to adopt to expand the subtree
     */
    public enum Strategy {

        UBFIRST,
        LBFIRST,
        DIFFFIRST,
    }
    protected Strategy strategy = Strategy.UBFIRST;

    public HeuristicPruningTree(Query query) {
        super(query);
    }

    public HeuristicPruningTree(Query query, Strategy strategy) {
        super(query);
        this.strategy = strategy;
    }

    public HeuristicPruningTree(Query query, int cardinality, TreeType type, Strategy strategy) {
        super(query, cardinality, type);
        this.strategy = strategy;
    }

    /*
     * Expand only one each time you iterate. This is easier.
     * We can optimize, if they are all equal do not apply any heuristic. 
     */
    @Override
    protected void buildIteratively() throws TreeException {
        //BEGIN-DECLARATIONS
        RelaxationNode rn;
        ChoiceNode cn;
        LinkedList<Node> queue = new LinkedList<>();
        Node n;
        Query q;
        //END-DECLARATIONS

        bounds = new HashMap<>();
        marked = new HashSet<>();
        //waiting = new LinkedList<Node>();
        expandableNodes = new HashSet<>();
        //actualLevel = 1.0;
        uncompletedBranches = new LinkedHashMap<>();
        nodes++;
        queue.add(root);
        try {
            if (!db.isConnected()) {
                db.connect();
            }
            switch (type) {
                case MAX_VALUE_AVG:
                case MAX_VALUE_MAX:
                    bounds.put(root, new Pair<>(0.0, db.getMaxBenefit()));
                    break;
                case MIN_EFFORT:
                    bounds.put(root, new Pair<>(1.0, query.size()));
                    break;
                case PREFERRED:
                    bounds.put(root, new Pair<>(0.0, pref.compute(query, Utilities.toBooleanQuery(query))));
                    break;
            }
            while (!queue.isEmpty()) {
                n = queue.poll();

                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) {
                    if (!marked.contains(n)) {
                        if (n instanceof RelaxationNode) {
                            //DAVIDE-MOD 10/07/2014 - (Modified also the condition below)
                            //Check the condition in constructRelaxationNodes function
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
                                        bounds.put(cn, new Pair<>(query.size(), query.size()));
                                        if (!marked.contains(n)) {
                                            nodes++;
                                        }
                                    }
                                }
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

                            //End of the level - Top of the queue is a relaxation node
                            if (queue.peek() instanceof RelaxationNode) {
                                //Update and prune
                                /*
                                 * FILL for the first time the uncompleted branches map,
                                 * We use it to understand which are complete and which not
                                 */
                                if (uncompletedBranches.isEmpty()) {
                                    for (Node node : root.getChildren()) {
                                        uncompletedBranches.put(node, (int) actualLevel);
                                    }
                                }
                                if (consideredBranch != null) {
                                    uncompletedBranches.put(consideredBranch, (int) actualLevel);
                                } else if (actualLevel != 1) {
                                    for (Node node : uncompletedBranches.keySet()) {
                                        uncompletedBranches.put(node, (int) actualLevel);
                                    }
                                }
                                update(queue);
//                                if (!marked.contains(n)) {
//                                    time += (System.nanoTime() - currentTime);
//
//                                }
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
        } catch (Exception ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        }
    }

// 15/07/2014: modified to match PruningTree
//    @Override
//    protected void updateBounds(RelaxationNode n, Pair<int[], double[]> resultSet) throws ConnectionException {
//        double lb, ub;
//
//        switch (type) {
//            case MAX_VALUE_AVG:
//                double sum = 0;
//                for (double benefit : resultSet.getSecond()) {
//                    sum += benefit;
//                }
//                if (!n.isEmpty()) {
//                    lb = ub = resultSet.getFirst().length == 0 ? 0 : sum / resultSet.getFirst().length;
//                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
//                else if (n.getQuery().allHardConstraints()) {
//                    lb = ub = 0;
//                } else {
//                    lb = 0;
//                    ub = db.getMaxBenefit();
//                    expandableNodes.add(n);
//                }
//                bounds.put(n, new Pair<>(lb, ub));
//                break;
//            case MAX_VALUE_MAX:
//                //double max = -(Double.MAX_VALUE); 
//                double max = 0;
//                for (double benefit : resultSet.getSecond()) {
//                    if (benefit > max) {
//                        max = benefit;
//                    }
//                }
//                if (!n.isEmpty()) {
//                    lb = ub = max;
//                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
//                else if (n.getQuery().allHardConstraints()) {
//                    if (!n.getQuery().getHardConstraints().isEmpty()) {
//                        Query q = new Query(n.getQuery().getHardConstraints());
//                        Pair<Double, Double> minMax = db.getMinMaxBenefit(q);
//                        //lb = minMax.getFirst();
//                        ub = minMax.getSecond();
//                    } else {
//                        ub = db.getMaxBenefit();
//                    }
//                    lb = 0;
//                } else {
//                    lb = 0;
//                    ub = db.getMaxBenefit();
//                    expandableNodes.add(n);
//                }
//                bounds.put(n, new Pair<>(lb, ub));
//                break;
//            case MIN_EFFORT:
//                if (!n.isEmpty() || n.getQuery().allHardConstraints()) {
//                    bounds.put(n, new Pair<>(actualLevel, actualLevel));
//                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
//                else {
//                    bounds.put(n, new Pair<>(actualLevel + 1, (double) query.size()));
//                    expandableNodes.add(n);
//                }
//                break;
//            case PREFERRED:
//                if (!n.isEmpty() || n.getQuery().allHardConstraints()) {
//                    bounds.put(n, new Pair<>(actualLevel, actualLevel));
//                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
//                else {
//                    bounds.put(n, new Pair<>(actualLevel + 1, (double) query.size()));
//                    expandableNodes.add(n);
//                }
//                break;
//        }
//    }

    
     /*
     * Updates the bounds tha are used to prune the nodes that for sure, won't 
     * lead to a promising path. 
     */
    // 15/07/2014: Modified 
    @Override
    protected void updateBounds(RelaxationNode n, Pair<int[], double[]> resultSet) throws ConnectionException {
        double lb, ub;
        Query q = new Query(n.query.getConstraints());
        Pair<Double, Double> nodeBounds = null;
        //double max = 0;

        switch (type) {
            case MAX_VALUE_MAX:
                lb = ub = 0;
                //double max = -(Double.MAX_VALUE);
                //double benefit;
                if (!n.isEmpty()) {
                    if (cachedBounds.containsKey(q)) {
                        ub = cachedBounds.get(q);
                    } else {
                        for (double benefit : resultSet.getSecond()) {
                            //benefit = t.getBenefit();
                            if (benefit > ub) {
                                ub = benefit;
                            }
                        }
                        cachedBounds.put(q, ub);
                    }
                    lb = ub;
                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
                else if (n.getQuery().allHardConstraints()) {
                    lb = ub = 0;
                } else {
                    // These are not tight bounds! 
                    expandableNodes.add(n);
                    if (!n.getQuery().getHardConstraints().isEmpty()) {
                        Query hq = new Query(n.getQuery().getHardConstraints());
                        if (cachedBounds.containsKey(hq)) {
                            ub = cachedBounds.get(hq);
                        } else {
                            Pair<Double, Double> minMax = db.getMinMaxBenefit(hq);
                            //lb = minMax.getFirst();
                            ub = minMax.getSecond();
                            cachedBounds.put(hq, ub);
                        }
                    } else {
                        ub = db.getMaxBenefit();
                    }
                    //ub = db.getMaxBenefit();
                    lb = 0; //This is the minimum value
                }
                nodeBounds = new Pair<>(lb, ub);
                //bounds.put(n, new Pair<Double, Double>(lb, ub));
                break;
            case MAX_VALUE_AVG:
                double sum = 0;
                lb = ub = 0;
                //double max = -(Double.MAX_VALUE);

                if (!n.isEmpty()) {
                    if (cachedBounds.containsKey(q)) {
                        ub = cachedBounds.get(q);
                    } else {
                        for (double benefit : resultSet.getSecond()) {
                            sum += benefit;
                        }
                        ub = resultSet.getFirst().length == 0 ? 0 : sum / resultSet.getFirst().length;
                        cachedBounds.put(q, ub);
                    }
                    lb = ub;
                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
                else if (n.getQuery().allHardConstraints()) {
                    lb = ub = 0;
                } else {
                    // These are not tight bounds! 
                    expandableNodes.add(n);
                    if (!n.getQuery().getHardConstraints().isEmpty()) {
                        Query hq = new Query(n.getQuery().getHardConstraints());
                        if (cachedBounds.containsKey(hq)) {
                            ub = cachedBounds.get(hq);
                        } else {
                            Pair<Double, Double> minMax = db.getMinMaxBenefit(hq);
                            //lb = minMax.getFirst();
                            ub = minMax.getSecond();
                            cachedBounds.put(hq, ub);
                        }
                    } else {
                        ub = db.getMaxBenefit();
                    }
                    //ub = db.getMaxBenefit();
                    lb = 0; //This is the minimum value
                }
                nodeBounds = new Pair<>(lb, ub);
                //bounds.put(n, new Pair<Double, Double>(lb, ub));
                break;
            case MIN_EFFORT:
                if (!n.isEmpty() || n.getQuery().allHardConstraints()) {
                    nodeBounds = new Pair<>(actualLevel, actualLevel);
                } // ... otherwise lb =  level + 1 (immediately stops) and ub = |Q| (stops at the end)
                else {
                    nodeBounds = new Pair<>(actualLevel + 1, (double) query.size());
                    //bounds.put(n, new Pair<Double, Double>(actualLevel + 1, (double) query.size()));
                    expandableNodes.add(n);
                }
                break;
            case PREFERRED:
                lb = ub = 0;
                if (!n.isEmpty()) {
                    if (cachedBounds.containsKey(q)) {
                        ub = cachedBounds.get(q);
                    } else {
                        double preference = 0;
                        //resultSet = db.submitQuery(n.query); 
                        ub = 0;
                        for (int t : resultSet.getFirst()) {
                            preference = pref.compute(query, t);
                            if (preference > ub) {
                                ub = preference;
                            }
                        }
                        cachedBounds.put(q, ub);
                    }
                    lb = ub;
                    nodeBounds = new Pair<>(lb, ub);
                    //bounds.put(n, new Pair<Double, Double>(lb, ub));
                } else if (n.getQuery().allHardConstraints()) {
                    nodeBounds = new Pair<>(0.0, 0.0);
                    //bounds.put(n,new Pair<Double, Double>(0.0, 0.0) );
                } else {
                    Query hq = new Query(n.getQuery().getHardConstraints());
                    expandableNodes.add(n);
                    if (cachedBounds.containsKey(hq)) {
                        ub = cachedBounds.get(hq);
                    } else {
                        double preference = 0;
                        int[] rs = db.submitQuery(hq);
                        ub = 0;
                        for (int t : rs) {
                            preference = pref.compute(query, t);
                            if (preference > ub) {
                                ub = preference;
                            }
                        }
                        cachedBounds.put(hq, ub);
                    }
                    nodeBounds = new Pair<>(0.0, ub);
//                    //bounds.put(n, new Pair<Double, Double>(0.0, ub));
                }
                break;
        }
        bounds.put(n, nodeBounds);
    }
    
    /*
     * The pruning phase in this version is slightly more complicate since we need to
     * explore one path and put the rest in a waiting list.
     */
    @Override
    protected void prune(LinkedList<Node> queue) {
        LinkedList<Node> tree = new LinkedList<>();
        LinkedList<Node> otherQueue = new LinkedList<>();
        double ubMin, lbMax;
        int level = 0;
        List<Node> siblings;
        Node n = null, sibling;
        boolean pruningCondition;

        try {
            tree.add(root);
            while (!tree.isEmpty()) { //Explore all the nodes
                n = tree.poll();
                siblings = n.getSiblings();
                //System.out.println("Node: " + n + "Siblings: " + siblings);
                ubMin = bounds.get(n).getSecond();
                lbMax = bounds.get(n).getFirst();
                for (int i = 0; i < siblings.size(); i++) {
                    sibling = siblings.get(i);
                    if (bounds.get(sibling).getSecond() < ubMin) {
                        ubMin = bounds.get(sibling).getSecond();
                    }
                    if (bounds.get(sibling).getFirst() > lbMax) {
                        lbMax = bounds.get(sibling).getFirst();
                    }
                } //END FOR
                siblings.add(n);
                for (int i = 0; i < siblings.size(); i++) {
                    sibling = siblings.get(i);
                    if (!sibling.getChildren().isEmpty()) {
                        tree.add(sibling.getChildren().get(0)); //Safety condition if you want to survive. 
                    }
                    /*
                     * If the father is marked than so the chilren OR
                     * lb > some ub - PRUNING CONDITION
                     */

                    pruningCondition = sibling instanceof ChoiceNode
                            && ((!type.isMaximize() && bounds.get(sibling).getFirst() > ubMin)
                            || (type.isMaximize() && bounds.get(sibling).getSecond() < lbMax));
                    if (pruningCondition) {
//                        System.out.println("UbMin: " + ubMin);
//                        System.out.println("Siblings: " + bounds.get(sibling).getFirst());
//                        System.out.println(sibling);
                        marked.add(sibling);
                        uncompletedBranches.remove(sibling);
                        expandableNodes.remove(sibling);
                    }
                    if (marked.contains(sibling.father)) {
                        marked.add(sibling);
                        //uncompletedBranches.remove(sibling.father);
                        uncompletedBranches.remove(sibling);
                        expandableNodes.remove(sibling);
                        //expandableNodes.remove(sibling.father);
                    }
                } //END FOR 
            }//END WHILE
            //Look at the best non-empty branch
            //System.out.println(marked);
            double value = 0.0, min = Double.MAX_VALUE, check = 0;
            boolean allEqual = false;
            int validBranches = 0;
            if (consideredBranch != null && uncompletedBranches.containsKey(consideredBranch) && (expandableNodes.isEmpty() || uncompletedBranches.get(consideredBranch) == query.size())) {
                //A:begin
                //System.out.println("A: the level of the removed ("+ consideredBranch+") one is="+ uncompletedBranches.get(consideredBranch));
                int tmp_sum = 1, tmp_prod = 0;
                for (int i_i = 0; i_i <= query.size() - 1; i_i++) {
                    tmp_prod = 0;
                    for (int j_i = (int) uncompletedBranches.get(consideredBranch); j_i <= i_i; j_i++) {
                        if (tmp_prod == 0) {
                            tmp_prod = 1;
                        }
                        tmp_prod *= query.size() - j_i;
                    }
                    tmp_sum += tmp_prod;
                }
                // System.out.println("A: from level "+ uncompletedBranches.get(consideredBranch) +" we pruned "+ tmp_sum +" nodes.");
                this.noPrunedNodes += tmp_sum;
                //A:end
                uncompletedBranches.remove(consideredBranch);
            }
            Set<Node> nList = uncompletedBranches.keySet();
            consideredBranch = null;
            for (Node node : nList) {
                validBranches++;
                switch (strategy) {
                    case UBFIRST:
                        value = type.isMaximize() ? 1 / bounds.get(node).getSecond() : bounds.get(node).getSecond();
                        break;
                    case LBFIRST:
                        //Store 1/lb to reorder the list in increasing order
                        value = type.isMaximize() ? bounds.get(node).getFirst() : 1 / bounds.get(node).getFirst();
                        break;
                    case DIFFFIRST:
                        value = bounds.get(node).getSecond() - bounds.get(node).getFirst();
                        //value = type.isMaximize()? 1/value : value;
                        break;
                }
                //The number of vaflid branches tells us if we have to consider the last one
                if (validBranches == 1) {
                    check = value;
                } else {
                    allEqual = allEqual && (check == value);
                }
                if (value < min) {
                    min = value;
                    consideredBranch = node;
                    expandableNodes.clear();
                }
            }
            if (consideredBranch != null) {
                actualLevel = uncompletedBranches.get(consideredBranch);
            }
            //IDEA: Maybe add a diversification (jumping) step.
            //Scroll down in the tree. 
            tree.clear();
            tree.add(consideredBranch);
            while (!tree.isEmpty()) {
                n = tree.poll();
                if (n instanceof RelaxationNode && n.isLeaf()) {
                    otherQueue.add(n);
                    queue.remove(n);
                }
                if (n != null && !n.isLeaf()) {
                    tree.addAll(n.getChildren());
                }
            }
            //waiting.addAll(queue);
            queue.clear();
            queue.addAll(otherQueue);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
        HeuristicPruningTree t = new HeuristicPruningTree(query, cardinality, tt, strategy);
        t.marked = new HashSet<Node>();
        return optimalTree(t);
    }
}
