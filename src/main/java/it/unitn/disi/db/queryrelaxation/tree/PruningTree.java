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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A relaxation Tree with pruning capabilities. The algorithm
 * works materializing the tree up to level L and then computing some upper and
 * lower bound for the nodes. The method prunes a subtree using the bounds as an
 * information. A branch should be pruned if the lower bound is not smaller than
 * all the other upper bounds.
 *
 * 24/01/13: Generalized to take into account different cost functions, the
 * bounds computation now takes into account benefits in the min-max bound.
 *
 * @author Davide Mottin
 */
public class PruningTree extends OptimalRelaxationTree {
    /*
     * The bounds (upper and lower) associated to each node
     */
    protected Map<Node, Pair<Double, Double>> bounds;
    /*
     * The level you have reached from the root
     */
    protected double actualLevel = 1.0;
    /*
     * The marked nodes to be pruned
     */
    public Set<Node> marked;
    /*
     * A hash map that caches bound computations
     */
    public Map<Query, Double> cachedBounds;
    
    //to erase
    private boolean writeInfo = false; // to erase

    public PruningTree(Query query) {
        super(query);
        cachedBounds = new HashMap<>();
    }

    public PruningTree(Query query, TreeType type) {
        this(query, 1, type);
    }

    public PruningTree(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type, 1, false);
        cachedBounds = new HashMap<>();
    }

    @Override
    protected void buildIteratively() throws TreeException {
        //BEGIN-DECLARATIONS
        RelaxationNode rn;
        ChoiceNode cn;
        LinkedList<Node> queue = new LinkedList<>();
        Node n;
        Query q;

        bounds = new HashMap<>();
        marked = new HashSet<>();
        actualLevel = 1.0;
        nodes++;
        queue.add(root);
        //END-DECLARATIONS

        //to erase
        int count = 0;
        long previousTime = System.currentTimeMillis(), current_Time = System.currentTimeMillis(),
                updateTime = 0, pruneTime = 0;
        //end to erase

        String lastProcessedNodeInfo = "";//added for debugging

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

                if (writeInfo) {
                    appendNodeInAFile("\nProcessing: " + n.toString() + "\n father:" + n.father + "\n");
                    if (n.father != null) {
                        appendNodeInAFile(" father of father:" + n.father.father + "\n");
                    }
                    lastProcessedNodeInfo = n.toString() + " :\n";
                    if (n.father != null) {
                        lastProcessedNodeInfo += " its father:" + n.father.toString() + "\n";
                        if (n.father.father != null) {
                            lastProcessedNodeInfo += " its father of the father:" + n.father.father.toString();
                        }
                    }
                    lastProcessedNodeInfo += "\n";
                    System.out.println(lastProcessedNodeInfo);
                }

                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) {
                    if (!marked.contains(n)) {
//                        currentTime = System.nanoTime();
                        if (n instanceof RelaxationNode) {
                            //END-DAVIDE-MOD (Modified also the condition below)
                            //DAVIDE: 14/07/2014 kept for memory
                            if (((RelaxationNode) n).isEmpty()) {
                                
                                for (Constraint con : n.getQuery().getConstraints()) {
                                    if (!con.isHard()) {
                                        cn = new ChoiceNode();
                                        cn.setFather(n);
                                        cn.setConstraint(con);
                                        cn.setQuery((Query) n.getQuery().clone());
                                        ((RelaxationNode) n).addNode(con.getAttributeName(), cn);
                                        queue.add(cn);
                                        // Put first upper bounds and lower bounds
                                        // bounds.put(cn, new Pair<Double,Double>(actualLevel, query.size()));
                                        // Just an initilization, it practically doesn't matter the value you put here. 
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

                            queue.add(rn);
                            if (!marked.contains(n)) {
                                nodes += 2;
                                relaxationNodes += 2;
                            }
                            //End of the level - Top of the queue is a relaxation node
                            if (queue.peek() instanceof RelaxationNode) {
                                //Update and prune
                                ++actualLevel;

                                if (writeInfo) {
                                    current_Time = System.currentTimeMillis(); // to erase
                                }
                                update(queue);
                                if (writeInfo) {
                                    updateTime += System.currentTimeMillis() - current_Time; //to erase
                                }
//                                if (!marked.contains(n)) {
//                                    time += (System.nanoTime() - currentTime);
//                                }
                                if (writeInfo) {
                                    current_Time = System.currentTimeMillis(); // to erase
                                }
                                prune(queue);
                                if (writeInfo) {
                                    pruneTime += System.currentTimeMillis() - current_Time; //to erase
                                }
                                if (verbose) {
                                    System.out.println(toString());
                                }
//                            }
                            }
                        }
                    }
                } //END IF NOT EMPTY QUERY

                //to erase
                if (writeInfo) {
                    if ((count % 1000) == 0) {
                        current_Time = System.currentTimeMillis();
                        appendInfoInAFile(count + " nodes were processed so far (both choice and relax nodes). The last 1000  nodes in "
                                + (current_Time - previousTime) + " ms; update time = " + updateTime + "; pruning time = " + pruneTime + " ms;\n "
                                + /*marked.size()+" marked nodes & "*/ +relaxationNodes + " relaxationNodes. \n\n");
                        previousTime = System.currentTimeMillis();
                        pruneTime = 0;
                        updateTime = 0;
                    }
                    count++;
                }
                //to erase
            }
            if (writeInfo) {
                appendInfoInAFile("\n_____________\nbuildIteratively done!\n");
            }
        } catch (Exception ex) {
            System.out.println("An error had occured in buildIteratively (class PruningTree).\n Last processed node:" + lastProcessedNodeInfo);
            //ex.printStackTrace();
            throw new TreeException("Wrong way to build the model, please check", ex);
        }
    }
    

    protected RelaxationNode constructRelaxationNodes(ChoiceNode n, boolean yes) throws ConnectionException {
        Query q;
        RelaxationNode rn;
        Double probability;
        Pair<int[], double[]> resultSet;

        q = (Query) n.getQuery().clone();
        //Worst case, only estimate
        if (yes) {
            q.relax(n.getConstraint());
            rn = new RelaxationNode(q);
//            if (type == TreeType.PREFERRED) {
//                probability = computeYesProbability(query, (RelaxationNode) n.father);
//            } else {
            probability = computeYesProbability(q, (RelaxationNode) n.father);
//            }
            //}
            //System.out.printf("Probability: %g, Query: %s\n", probability, q.toString());
            n.setYesNode(probability, rn);
            if (db != null) { //Optimize, no node for sure are empty ;-)
                //MODIFIED - Nocturnum delirium
                resultSet = db.resultsAndBenefits(q);
                rn.setEmpty(resultSet.getFirst().length < cardinality);
                //Set the bounds. 
                //if it is a leaf the ub and lb are equal to the level ..
                updateBounds(rn, resultSet);
            }
        } else {
            for (Constraint con : q.getConstraints()) {
                if (con.equals(n.getConstraint())) {
                    con.setHard(true);
                }
            }
            rn = new RelaxationNode(q);
            rn.setEmpty(true);
            //DAVIDE-MOD - 10/07/2014 - 'No' nodes are FOR SURE empty, thing about it. 
//            resultSet = db.resultsAndBenefits(q);
//            System.out.println(q + ", size: " + resultSet.getFirst().length);
//            if (db != null) {
//                rn.setEmpty(resultSet.getFirst().length < cardinality);
//            }

            //DAVIDE-MOD - 02/06/2013 - Modified upper bound (hard constraint max)
            //Update: 10/07/2014 - 
            //If it returns no answers removing all the non hard, non empty
            //contraints, than it is a leaf, then it is marked nonEmpty 
            //(since we want to be a leaf in the next iteration)

            //qConstraints.clear();
            q = new Query();
            for (Constraint con : rn.query.getConstraints()) {
                if (con.isHard()) {
                    q.addConstraint(con);
                }
            }
            q.negatedConstraints().addAll(rn.query.negatedConstraints());
            rn.setEmpty(db.submitQuery(q).length >= cardinality); // Look the condition is reversed
            //END-DAVIDE-MOD (Modified the below condition) - added "!rn.isEmpty()"
            updateBounds(rn, new Pair<>(new int[0], new double[0]));
        }
        rn.setFather(n);
        return rn;
    }

    /*
     * Updates the bounds tha are used to prune the nodes that for sure, won't 
     * lead to a promising path. 
     */
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
     * Update the tree bounds bottom-up, like computing the cost but in a pruning
     * fashion way.
     */
    protected void update(LinkedList<Node> queue) {
        //A shallow copy is enough for us.
        LinkedList<Node> partialTree = (LinkedList<Node>) queue.clone();
        Node n;
        Pair<Double, Double> lbub;
        Pair<Double, Double> childBounds;
        Collection<? extends Node> children;
        ChoiceNode cn;
        double newLb, newUb;

        while (!partialTree.isEmpty()) {
            n = partialTree.poll();
            if (n instanceof RelaxationNode) {
                if (!n.isLeaf()) {
                    //Initialize bounds
                    newUb = newLb = type.isMaximize() ? 0 : Double.MAX_VALUE;
                    children = n.getChildren();
                    for (Node child : children) {
                        childBounds = bounds.get(child);
                        if (type.isMaximize()) {
                            if (childBounds.getFirst() > newLb) {
                                newLb = childBounds.getFirst();
                            }
                            if (childBounds.getSecond() > newUb) {
                                newUb = childBounds.getSecond();
                            }
                        } else {
                            if (childBounds.getFirst() < newLb) {
                                newLb = childBounds.getFirst();
                            }
                            if (childBounds.getSecond() < newUb) {
                                newUb = childBounds.getSecond();
                            }
                        }
                    }
                    lbub = bounds.get(n);
                    lbub.setFirst(newLb);
                    lbub.setSecond(newUb);
                }
            } else if (n instanceof ChoiceNode) {
                cn = (ChoiceNode) n;
                lbub = bounds.get(n);
                lbub.setFirst(
                        cn.getYesProbability() * bounds.get(cn.getYesNode()).getFirst()
                        + cn.getNoProbability() * bounds.get(cn.getNoNode()).getFirst()); //LB = p_yes * lb(yes_node) + p_no * lb(no_node)
                lbub.setSecond(
                        cn.getYesProbability() * bounds.get(cn.getYesNode()).getSecond()
                        + cn.getNoProbability() * bounds.get(cn.getNoNode()).getSecond()); //UB = p_yes * ub(yes_node) + p_no * ub(no_node)
            } else {
                throw new AssertionError("Wrong type of node in the tree");
            }
            if (n != root && partialTree.peekLast() != n.father) {
                //!partialTree.contains(n.father)) {
                partialTree.add(n.father);
            }
        } //END WHILE
    }


    /*
     * To prune start from the root and then mark the nodes till you get in some
     * of the enqueued nodes. Remove (or keep) the node from the list and continue.
     */
    protected void prune(LinkedList<Node> queue) {
        LinkedList<Node> tree = new LinkedList<>();
        double ubMin, lbMax;
        List<Node> siblings;
        Node n, sibling;

        tree.add(getCurrentRoot());
        while (!tree.isEmpty()) { //Explore all the nodes
            n = tree.poll();
            siblings = n.getSiblings();
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
            }
            siblings.add(n);
            for (int i = 0; i < siblings.size(); i++) {
                sibling = siblings.get(i);
                if (!sibling.getChildren().isEmpty()) {
                    tree.add(sibling.getChildren().iterator().next());
                }
                //If the father is marked than so the chilren OR
                //lb > some ub
                if (!type.isMaximize() && bounds.get(sibling).getFirst() > ubMin && sibling instanceof ChoiceNode) {
                    marked.add(sibling);
                }
                if (type.isMaximize() && bounds.get(sibling).getSecond() < lbMax && sibling instanceof ChoiceNode) {
                    marked.add(sibling);
                }
                if (marked.contains(sibling.father)) {
                    marked.add(sibling);
                }
            }//END FOR
        }//END WHILE
    }

    @Override
    protected boolean isMarked(Node n) {
        return marked.contains(n);
    }

    //to erase, it just write info in a file
    public void appendInfoInAFile(String tmp) { //to erase
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("information.txt", true));
            out.append(tmp);

        } catch (Exception ex) {
            System.out.println("Cannot write in the information.txt file.");
        } finally {

            try {
                out.close();
            } catch (Exception ex) {
            }
        }
    }

    public void appendNodeInAFile(String tmp) { // to erase
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("treeNodes.txt", true));
            out.append(tmp);
        } catch (Exception ex) {
            System.out.println("Cannot write in the information.txt file.");
        } finally {
            try {
                out.close();
            } catch (Exception ex) {
            }
        }
    }

    /**
     * This computes the cost of a node, depending on the cost of the subtrees
     * if the node is a <code>ChoiceNode</code> the cost is (c(yes) + 1)*p_yes +
     * (c(no) + 1)*p_no, if it is a <code>RelaxationNode</code> the cost is max
     * (c[q']), where q' is a direct subquery of q. Keep into account also
     * marked nodes
     *
     * @param n The node to update the cost.
     * @throws it.unitn.disi.db.queryrelaxation.tree.TreeException
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
            //double max = -(Double.MAX_VALUE);
            double max = 0;
            for (Node child : n.getChildren()) {
                if (!marked.contains(child)) {
                    if (child.getCost() < min) {
                        min = child.getCost();
                    }
                    if (child.getCost() > max) {
                        max = child.getCost();
                    }
                }
            }
            n.setCost(type.isMaximize() ? max : min);
        }
    }

    @Override
    protected String printPaths(Node n, String acc, boolean optimal) {
        if (n instanceof RelaxationNode) {
            if (n.isLeaf()) {
                return acc + "\n";
            } else {
                String result = "";
                for (Node child : n.getChildren()) {
                    if ((!optimal || n.cost == child.cost) && !marked.contains(child)) { //Take the paths with the cost of the root equal to the cost of the parent - i.e. Opt
                        result += printPaths(child, acc, optimal);
                    }
                }
                return result;
            }
        } else {
            //ChoiceNode cannot be a leaf
            ChoiceNode cn = (ChoiceNode) n;
            return printPaths(cn.getYesNode(), acc + cn.getConstraint().getAttributeName() + "|" + "yes" + "|", optimal)
                    + printPaths(cn.getNoNode(), acc + cn.getConstraint().getAttributeName() + "|" + "no" + "|", optimal);
        }
    }

    public boolean isPruned(Node n) {
        return marked.contains(n);
    }

    protected Node getCurrentRoot() {
        return root; 
    }
    
    @Override
    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
        PruningTree t = new PruningTree(query, cardinality, tt);
        t.marked = new HashSet<>();
        return optimalTree(t);
    }

    @Override
    protected boolean optimalityCondition(Node n1, Node n2) {
        return n1.getCost() == n2.getCost() && !marked.contains(n2) && !marked.contains(n1);
    }

    @Override
    public long getTime() {
        return time.getElapsedTimeMillis();
    }
}
