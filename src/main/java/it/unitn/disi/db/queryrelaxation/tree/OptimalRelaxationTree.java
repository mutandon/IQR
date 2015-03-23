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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

    
    /**
 * This class represents the relaxation tree used to represent all the posssible
 * relaxations. 
 * 
 * 24/01/13: Generalized to take into account different cost functions
 * 
 * @author Davide Mottins
 * @see Node
 * @see RelaxationNode
 * @see ChoiceNode
 */
public class OptimalRelaxationTree extends RelaxationTree {
    /*
     * An edge in dot language
     */
    protected static final String EDGE = "\t\"%d\" -> \"%d\" [label=\"%s\"];\n";
    protected static final String PROB_EDGE = "\t\"%d\" -> \"%d\" [label=\"%s%.3f\", arrowhead=ediamond];\n";
    /*
     * A node in dot language
     */
    protected static final String NODE = "\t\"%d\" [label = \"%d%s\",shape = %s,fillcolor = %s,style=filled];\n";
    /*
     * This are the already computed probabilities
     */
    protected Map<Integer, Double> computedProbabilities;
    /*
     * Number of pruned nodes
     */
    protected int noPrunedNodes = 0;
    /*
     * Number of relaxation nodes
     */
    protected int relaxationNodes = 0;
    /*
     * Time spent in IPF interrogartion
     */
    private long totalTimeIPFInterrogation; //A:
    /*
     * Parameter c controls the penalization at each step (look at cost-function) 
     */
    protected double c; 
    /*
     * The default tree type is the min effort tree
     */
    public static final TreeType DEFAULT_TYPE = TreeType.MIN_EFFORT;
    /*
     * Represents the cost of an empty query 
     */
    protected Map<Query, Double> cachedResults;
        
    
    /**
     * Construct the root node of the tree using the input query. To populate
     * the tree <code>materialize()</code> or <code>materializeIteratively()</code>
     * must be called.
     * @param query The query to be associated to the root node
     * @param cardinality
     * @param type
     */
    public OptimalRelaxationTree(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type);
        root = new RelaxationNode(query);
        cachedResults = new HashMap<>();
        computePenalty();
        totalTimeIPFInterrogation = 0;//A:
    }

    public OptimalRelaxationTree(Query q) {
        this(q, 1, DEFAULT_TYPE);
    }
    

    /**
     * Build the whole tree starting by the root using the database, if any.
     * @param computeCosts True if you want to also compute costs, otherwise
     * you can call <code>computeCosts</code> function separately.
     * @throws TreeException If the tree construction generates an error.
     */
    public void materialize(boolean computeCosts) throws TreeException {
        computedProbabilities = new HashMap<>();
        time.reset();
        time.start();
        buildIteratively();
        
        if (computeCosts) {
            computeCosts();
        }
        time.stop();
    }

    /**
     * Materialize the tree in an iterative way. Slower but requires less memory
     * consumption
     * @throws TreeException If something wrong happens while constructing the tree
     */
    protected void buildIteratively() throws TreeException {
        RelaxationNode rn;
        ChoiceNode cn;
        Query q;
        Double probability, den, num;
        LinkedList<Node> queue = new LinkedList<>();
        boolean leaf;
        Map<Integer, Constraint> qConstraints = new HashMap<>();
        int[] results; 
        Node n;
        queue.add(root);
        nodes = 1;
        relaxationNodes = 1;
        try {
            if (!db.isConnected()) {
                db.connect();
            }
            while (!queue.isEmpty()) {
                n = queue.poll();
                //No further relaxations or only hard constraints. 
                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) { 
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
                                    nodes++;
                                }
                            }
                        }
                    } 
                    else if (n instanceof ChoiceNode) {
                        //Build yes node
                        q = (Query) n.getQuery().clone();
                        q.relax(((ChoiceNode) n).getConstraint());
                        rn = new RelaxationNode(q);

                        probability = computeYesProbability(q, (RelaxationNode) n.father);
                        
                        ((ChoiceNode) n).setYesNode(probability, rn);
                        rn.setFather(n);
                        
                        results = db.submitQuery(q);
                        //Cardinality constraint acts as a stopping condition
                        rn.setEmpty(results.length < cardinality);
                        queue.add(rn);
                        relaxationNodes++;
                        nodes++;
                        //Build 'no' node
                        q = (Query) n.getQuery().clone();
                        for (Constraint c : q.getConstraints()) {
                            if (c.equals(((ChoiceNode) n).getConstraint())) {
                                c.setHard(true);
                            }
                        }
                        rn = new RelaxationNode(q);
                        rn.setFather(n);
                        //DAVIDE-MOD
                        //DAVIDE-MOD (10/07/2014) - The reasoning is the opposite: 
                        //If it returns no answers removing all the non hard, non empty
                        //contraints, than it is a leaf, then it is marked nonEmpty 
                        //(since we want to be a leaf in the next iteration)
                        qConstraints.clear();
                        q = new Query();
                        for (Constraint c : rn.query.getConstraints()) {
                            if (c.isHard()) {
                                q.addConstraint(c);
                            }
                        }
                        q.negatedConstraints().addAll(rn.query.negatedConstraints());
                        //If the query gives us some result the node is not empty
                        results = db.submitQuery(q);
                        rn.setEmpty(results.length >= cardinality);
                        //DAVIDE-MOD-END
                        ((ChoiceNode) n).setNoNode(1 - probability, rn);
                        //((ChoiceNode) n).setNoNode(computeNoProbabilitySecondVersion(q, (RelaxationNode) n.father), rn);
                        queue.add(rn);
                        relaxationNodes++;
                        nodes++;
                    }
                } //END IF NOT EMPTY QUERY
            }
        } catch (Exception ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        } 
    }

    /**
     * Visit the tree in preorder, i.e. first visit the root and the children,
     * and returns the list of all the nodes
     * @return The list of nodes.
     */
    public List<Node> visit() {
        List<Node> nodes = new ArrayList<>();
        visit(nodes, root);
        return nodes;
    }

    /*
     * Recursive visit of the tree.
     */
    private void visit(List<Node> nodes, Node root) {
        if (root == null) {
            return;
        }
        nodes.add(root);
        for (Node child : root.getChildren()) {
            visit(nodes, child);
        }
    }

    /*
     * Auxiliary function to visit the nodes and store them in a DOT language
     * fashion.
     */
    protected void visit(Set<String> edges, Map<Integer, String> nodes) {
        int index = 1, rindex;
        LinkedList<Node> queue = new LinkedList<>();
        LinkedList<Integer> roots = new LinkedList<>();
        ChoiceNode cn;
        String shape = "";
        Node n;

        queue.add(root);
        roots.add(index);
        while (!queue.isEmpty()) {
            n = queue.poll();
            rindex = roots.poll();
            if (n instanceof RelaxationNode) {
                shape = "circle";
                for (Node child : n.getChildren()) {
                    edges.add(String.format(EDGE, rindex, ++index, ((ChoiceNode) child).getConstraint().getAttributeName()));
                    queue.add(child);
                    roots.add(index);
                }
            } else if (n instanceof ChoiceNode) {
                shape = "triangle";
                cn = (ChoiceNode) n;
                edges.add(String.format(PROB_EDGE, rindex, ++index, "Yes=", cn.getYesProbability()));
                roots.add(index);
                edges.add(String.format(PROB_EDGE, rindex, ++index, "No=", cn.getNoProbability()));
                roots.add(index);
                queue.addAll(n.getChildren());
            }
            nodes.put(rindex, String.format(NODE, rindex, rindex, n.cost >= 0 ? String.format("\\nc=%.4g", n.cost) : "", shape, isMarked(n) ? "red" : "white"));
        }
    }
    
    /*
     * Compute the probability for a user to say no to a relaxation.= (1-pref)*prior
     */
    protected double computeNoProbability(Query q1, RelaxationNode parent) throws ConnectionException {
        //new version
        double probability = 0.0;
        Double pr = null;

        int t = Utilities.toBooleanQuery(q1);
        long curentTime = System.nanoTime(); //A:
        pr = prior.getProbability(t);
        totalTimeIPFInterrogation += System.nanoTime() - curentTime; //A:
        if (type == TreeType.PREFERRED)
            probability = (1 - pref.compute(query, t)) * pr;
        else
            probability = (1 - pref.compute(parent.getQuery(), t)) * pr;
        return probability;
    }

    protected double computeYesProbability(Query q1, RelaxationNode parent) throws ConnectionException {
        return 1 - computeNoProbability(q1, parent);
    }

    
    /*
    @Deprecated
    protected double computeYesProbability(Query q1) throws ConnectionException {
        //new version
        double probability = 0.0;
        double pr = 0.0;
        double denominator = 0.0;

        //if the query has relaxed attributes, add them
        Tuple t = new Tuple(new Query(q1.constraintsAndNegations()));

        //  if(t.size() != root.getQuery().size()) System.out.println("this query is wrong"+q1);; // to erase
        long curentTime = System.nanoTime(); //A:

        pr = prior.getProbability(t);

        totalTimeIPFInterrogation += System.nanoTime() - curentTime; //A:


        denominator = pr;
        probability = (pr * pref.compute(root.getQuery(), t)) / pr;

        //      System.out.println("Ham("+root.getQuery()+"t)="+pref.compute(root.getQuery(), t)+ " prob="+probability);
        //      System.out.println("Q1:"+ q1.toString()+ " tuple= "+ t+
        //             "\nden="+denominator+ "prob="+probability+ "pref ="+ pref.compute(root.getQuery(),t) );

        //if (verbose)
        //   System.out.printf("Q1: %s, Q: %s, Denominator: %s, Probability: %f, R': %s", q1.toString(), q.toString(), denominator, probability, rQ1.toString());
        computedProbabilities.put(q1.hashCode(), probability);
        return probability;
    }

    @Deprecated
    protected double computeYesProbabilityOLD(Query q1) throws ConnectionException {
        // old version

        double probability = 0.0;
        double pr = 0.0;
        double denominator = 0.0;
        //db.connect();
        List<Tuple> rQ1 = db.queryTupleSpace(q1, true);
        Query q = root.getQuery();


        for (int i = 0; i < rQ1.size(); i++) {
            pr = prior.getProbability(rQ1.get(i));
            denominator += pr;
            probability += (pr * pref.compute(q, rQ1.get(i)));
        }
        //db.close();
        probability /= denominator;
        if (verbose) {
            System.out.println(String.format("Q1: %s, Q: %s, Denominator: %s, Probability: %f, R': %s", q1.toString(), q.toString(), denominator, probability, rQ1.toString()));
        }
        computedProbabilities.put(q1.hashCode(), probability);
        return probability;
    }
    */

    private void computePenalty() {
        switch (type) {
            case MAX_VALUE_AVG:
            case MAX_VALUE_MAX:
            case PREFERRED: 
                c = 0;
                break;
            case MIN_EFFORT : 
                c = 1;
                break;
            default:
                throw new AssertionError();
        }
    }
    
    /**
     * Compute costs in a bottom-up iterative fashion traversing the tree in pre-order
     * and updating the costs, using <code>updatetCost</code> function from the
     * leaves to the root.
     * @throws TreeException If it is not possible to compute the cost
     */
    @Override
    public void computeCosts() throws TreeException {
        LinkedList<Node> stack = new LinkedList<>();
        LinkedList<Integer> currentChild = new LinkedList<>();
        Node currentNode, child;
        Integer actualChild;

        //Idea to optimize: have a stack to get the next child to visit --
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
                    //TODO: Update this, set value (must be an input for tuples)
                    computeLeafCost((RelaxationNode) currentNode);
                } else if (currentNode.getChildren().size() <= actualChild) {//Visited all the childs
                    stack.pop();
                    updateCost(currentNode);
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
     * Leaf cost is computed differently among different cost function. The static
     * value-based function use the value to get the benefit of selecting one
     * value instead of another
     * 
     * @param n The leaf node to compute
     * @throws ConnectionException In case the database is not available.
     */
    protected void computeLeafCost(RelaxationNode n) throws ConnectionException {
        assert n.isLeaf() : "Node must be a leaf"; 
        Pair<int[],double[]> resultSet;
        //double max = 0; 
        double cost = 0;
        Query q = new Query(n.query.getConstraints());
        
        if (!n.isEmpty()) {
            switch (type) {
                case MAX_VALUE_AVG:
                    if (cachedResults.containsKey(q))
                        cost = cachedResults.get(q);
                    else {
                        resultSet = db.resultsAndBenefits(q);
                        double sum = 0; 
                        for (double t : resultSet.getSecond()) {
                            sum += t;
                        }
                        cost = resultSet.getFirst().length != 0? sum/resultSet.getFirst().length : 0;
                        cachedResults.put(q, cost);
                    }
                    break;  
                case MAX_VALUE_MAX :
                    if (cachedResults.containsKey(q))
                        cost = cachedResults.get(q);
                    else {
                        resultSet = db.resultsAndBenefits(n.query); 
                        for (double benefit : resultSet.getSecond()) {
                            if (benefit > cost)
                                cost = benefit;
                        }
                        cachedResults.put(q, cost);
                    }
                    break;
                case MIN_EFFORT : 
                    break;
                case PREFERRED : 
                    if (cachedResults.containsKey(q))
                        cost = cachedResults.get(q);
                    else {
                        double preference;
                        resultSet = db.resultsAndBenefits(n.query); 
                        for (int t : resultSet.getFirst()) {
                            preference = pref.compute(query, t);
                            if (preference > cost)
                                cost = preference;
                        }
                        cachedResults.put(q, cost);
                        //n.setCost(max);                
                    }
                    break;
                default:
                    throw new AssertionError("Wrong type");
            }
        }
        n.setCost(cost);
    }
    
    
    /**
     * This computes the cost of a node, depending on the cost of the subtrees
     * if the node is a <code>ChoiceNode</code> the cost is (c(yes) + 1)*p_yes +
     * (c(no) + 1)*p_no, if it is a <code>RelaxationNode</code> the cost is
     * max (c[q']), where q' is a direct subquery of q. 
     * @param n The node to update the cost.
     * @throws it.unitn.disi.db.queryrelaxation.tree.TreeException
     * @see RelaxationNode
     */
    public void updateCost(Node n) throws TreeException {
        //TODO: IF GENERIC CHANGE HERE + 1 -> +c, min -> max
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());
        } else if (n instanceof RelaxationNode) {
            double min = Double.MAX_VALUE;
            double max = -(Double.MAX_VALUE);
            for (Node child : n.getChildren()) {
                if (child.getCost() < min) 
                    min = child.getCost();
                if (child.getCost() > max)
                    max = child.getCost();
            }
            n.setCost(type.isMaximize()? max : min);
        }
    }

    /**
     * Time spent in computations (in milliseconds)
     * @return The time expressed in milliseconds
     */
    @Override
    public long getTime() {
        long t = time.getElapsedTimeMillis();
        if (type == TreeType.MIN_EFFORT) {
            t = t/(long)root.cost;
        }
        return t;
    }


    /**
     * A method to represent the actual tree in a string encoded in Graphviz DOT language
     * @return A string representing the tree
     * @see <a href="http://www.graphviz.org/">Graphviz</a>
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Set<String> tree = new LinkedHashSet<>();
        Map<Integer, String> costs = new LinkedHashMap<>();

        visit(tree, costs);
        sb.append("digraph relaxation_graph {\n");
        sb.append("\tsize=\"8,5\"\n");
        for (Integer cost : costs.keySet()) {
            sb.append(costs.get(cost));
        }
        for (String node : tree) {
            sb.append(node);
        }
        sb.append("}\n");

        return sb.toString();
    }

    /**
     * Return the number of pruned nodes
     * @return Number of pruned nodes
     */
    public int getNoPrunedNodes() { 
        return this.noPrunedNodes;
    }

    /**
     * Return the number of relaxation nodes
     * @return Number of relaxation nodes
     */
    public int getRelaxationNodes() {
        return this.relaxationNodes;
    }

    /**
     * Return the total time spent in interrogating the IPF
     * @return 
     */
    public long getTotalTimeIPFInterrogation() { 
        return this.totalTimeIPFInterrogation;
    }

    /**
     * Reset the time for the interrogation of the IPF
     */
    @Override
    public void resetTime() {
        this.totalTimeIPFInterrogation = 0;
    }
    
    /**
     * Produce an HashSet of strings with all the optimal paths of the tree. 
     * The path has syntax
     * 
     * path := constraint\[choice\]*
     * constraint := STRING
     * choice := Y|N
     * 
     * To signal all possible choices.
     * @return a HashSet containing the paths
     */
    public Set<String> optimalPaths() {
        return splitPahts(true);
    }
    
    /**
     * Produce an HashSet of strings with all the paths of the tree. 
     * The path has syntax
     * 
     * path := constraint\[choice\]*
     * constraint := STRING
     * choice := Y|N
     * 
     * To signal all possible choices.
     * @return a HashSet containing the paths
     */    
    public Set<String> allPaths() {
        return splitPahts(false);
    }
    
    /*
     * Split the paths produced with method printPaths
     */
    protected Set<String> splitPahts(boolean optimal) {
        String[] paths = printPaths(root, "", optimal).split("\n");
        Set<String> retval = new HashSet<>();
        for (int i = 0; i < paths.length; i++) {
            retval.add(paths[i].trim());
        }
        return retval;
    }
    
    /*
     * Returns the optimal tree associated with the current tree, this methods
     * is useful when you want to compare two optimal trees. 
     * The class returned by the method is the same as this tree. 
     * @param root 
     * @return null if it is not possible to create the tree otherwise returns
     * the optimal tree
     */
    @Override
    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
        return optimalTree(new OptimalRelaxationTree(query, cardinality, tt));
    }
    
    protected RelaxationTree optimalTree(RelaxationTree optTree) throws TreeException {
        LinkedList<Node> queue = new LinkedList<>();
        LinkedList<Node> optQueue = new LinkedList<>();
        Map<String, ChoiceNode> relaxations;
        try {
            optTree.prior = this.prior;
            optTree.pref = this.pref;
            optTree.db = this.db;
            
            RelaxationNode rn;
            ChoiceNode cnOpt;
            ChoiceNode cnOrig;
            Node nOrig, nOpt;
            queue.add(root);
            optQueue.add(optTree.root);
            while (!queue.isEmpty()) {
                nOrig = queue.poll();
                nOpt = optQueue.poll();
                assert (nOpt.getClass().getCanonicalName().equals(nOrig.getClass().getCanonicalName())) : "Optimal node and original node have a different class";
                if (nOrig instanceof RelaxationNode) {
                    if (!nOrig.isLeaf()) {
                        //new code with hard constr stop
                        //DAVIDE-MOD
                        rn = (RelaxationNode)nOrig;
                        relaxations = rn.getRelaxations();
                        for (String label : relaxations.keySet()) {
                            cnOrig = relaxations.get(label);
                            if (optimalityCondition(nOrig, cnOrig)) {
                                cnOpt = (ChoiceNode) cnOrig.clone();
                                cnOpt.setFather(nOpt);
                                ((RelaxationNode)nOpt).addNode(label, cnOpt);
                                queue.add(cnOrig);
                                optQueue.add(cnOpt);
                            }
                        }
                    }
                }
                else if (nOrig instanceof ChoiceNode) {
                    //Build yes node
                    cnOrig = (ChoiceNode)nOrig;
                    cnOpt = (ChoiceNode)nOpt;

                    //No node
                    rn = (RelaxationNode)cnOrig.getNoNode().clone();
                    rn.setFather(cnOpt);
                    cnOpt.setNoNode(cnOrig.getNoProbability(), rn);
                    queue.add(cnOrig.getNoNode());
                    optQueue.add(rn);

                    //Yes node
                    rn = (RelaxationNode)cnOrig.getYesNode().clone();
                    rn.setFather(cnOpt);
                    cnOpt.setYesNode(cnOrig.getYesProbability(), rn);
                    queue.add(cnOrig.getYesNode());    
                    optQueue.add(rn);   
                }
            } //END IF NOT EMPTY QUERY
        } catch (Exception ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        }
        return optTree;
    }
    
    
    @Override
    protected boolean optimalityCondition(Node n1, Node n2) {
        return n1.cost == n2.cost;
    }

    /* 
     *  None of the nodes are marked in the optimal tree. 
     */
    @Override
    protected boolean isMarked(Node n) {
        return false;
    }
    
    /*
     * Recursive function to produce all the paths separated by a \n
     */
    protected String printPaths(Node n, String acc, boolean optimal) {
        if (n instanceof RelaxationNode) {
            if (n.isLeaf()) {
                return acc + "\n";
            } else {
                String result = "";
                for (Node child : n.getChildren()) {
                    if (!optimal || n.cost == child.cost) { //Take the paths with the cost of the root equal to the cost of the parent - i.e. Opt
                        result += printPaths(child, acc, optimal);
                    }
                }
                return result;
            }            
        } else {
            //ChoiceNode cannot be a leaf
            ChoiceNode cn = (ChoiceNode)n;
            return printPaths(cn.getYesNode(), acc + cn.getConstraint().getAttributeName() + "|" + "yes" +  "|", optimal) +
            printPaths(cn.getNoNode(), acc + cn.getConstraint().getAttributeName() + "|" + "no" + "|", optimal);
        }
    }
    
    /**
     * Return the candidate relaxation nodes belonging to some Optimal path
     * @return An HashSet of nodes that belong to some optimal path
     */
    public Set<Node> getCandidateOptimalNodes() {
        LinkedList<Node> queue = new LinkedList<>();
        ChoiceNode cn;
        Node n;
        Set<Node> optPath = new HashSet<>();

        queue.add(root);
        while (!queue.isEmpty()) {
            n = queue.poll();
            if (n instanceof RelaxationNode) {
                for (Node child : n.getChildren()) {
                    if (n.cost == child.cost) { //Take the paths with the cost of the root equal to the cost of the parent
                        optPath.add(child);
                        queue.add(child);
                        //Break if you want the lefmost
                    }
                }
            } else if (n instanceof ChoiceNode) {
                //Add all children of a choice node.
                queue.addAll(n.getChildren());
                
            }
        }
        return optPath;
    }

}
