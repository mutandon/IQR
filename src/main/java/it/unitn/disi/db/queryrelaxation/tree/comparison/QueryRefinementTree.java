/*
 * IQR (Interactive Query Relaxation) Library
 * Copyright (C) 2012  Davide Mottin (mottin@disi.unitn.eu
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
package it.unitn.disi.db.queryrelaxation.tree.comparison;

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.BooleanMockConnector;
import it.unitn.disi.db.queryrelaxation.model.functions.IPFPrior;
import it.unitn.disi.db.queryrelaxation.model.functions.IdfFunction;
import it.unitn.disi.db.queryrelaxation.statistics.EmptyQueryGeneration;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import it.unitn.disi.db.queryrelaxation.tree.ChoiceNode;
import it.unitn.disi.db.queryrelaxation.tree.Node;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationNode;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.SimpleNode;
import it.unitn.disi.db.queryrelaxation.tree.TreeException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Query Refinement tree is an implementation of "Interactive Query Refinement" [1] 
 * paper reduced to our problem as described in [out paper]
 * 
 * [1] Chaitanya Mishra, Nick Koudas: Interactive query refinement. EDBT 2009:862-873
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class QueryRefinementTree extends RelaxationTree {
    /*
     * An edge in dot language
     */
    protected static final String EDGE = "\t\"%d\" -> \"%d\" [label=\"%s\"];\n";
    protected static final String PROB_EDGE = "\t\"%d\" -> \"%d\" [label=\"%s[%.3f]\", arrowhead=ediamond];\n";
    /*
     * A node in dot language
     */
    protected static final String NODE = "\t\"%d\" [label = \"%d%s\",shape = %s,fillcolor = %s,style=filled];\n";
    /*
     * This are the already computed probabilities
     */
    protected Map<Integer, Double> computedProbabilities;

    private Set<String> nonEmptyQueries = new LinkedHashSet<>(); 
    
    /**
     * Construct the root node of the tree using the input query. To populate
     * the tree <code>materialize()</code> or <code>materializeIteratively()</code>
     * must be called.
     * @param query The query to be associated to the root node
     */
    public QueryRefinementTree(Query query) {
        super(query);
        root = new SimpleNode(query);
    }

    /**
     * Build the whole tree starting by the root using the database, if any.
     * @param computeCosts True if you want to also compute costs, otherwise
     * you can call <code>computeCosts</code> function separately.
     * @throws TreeException If the tree construction generates an error.
     */
    @Override
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
        Query q;
        LinkedList<SimpleNode> queue = new LinkedList<>();
        SimpleNode n, nn;
        List<Constraint> constraints;
        Map<Constraint, Double> probs;
        
        queue.add((SimpleNode)root);
        nodes = 1;
        try {
            if (!db.isConnected()) {
                db.connect();
            }
            while (!queue.isEmpty()) {
                n = queue.poll();
                if (!n.getQuery().getConstraints().isEmpty()) {
                    if (n.isEmpty()) {
                        constraints = n.getQuery().getConstraints();
                        //Add probabilities
                        probs = computeRelaxationProbabilities(n.getQuery());
                        for (Constraint c : constraints) {
                            q = (Query) n.getQuery().clone();
                            q.relax(c);
                            nn = new SimpleNode(q);
                            nn.setFather(n);
                            nn.setRelaxation(c.getAttributeName());
                            //nn.setQuery(q);
                            nn.setProbability(probs.get(c));
                            n.addChild(nn);
                            queue.add(nn);
                            if (db != null) {
                                nn.setEmpty(db.submitQuery(q).length == 0);
                            } if (!nn.isEmpty()) {
                                nonEmptyQueries.add(EmptyQueryGeneration.queryToString(q));
                            }
                        
                            nodes++;
                        }
                    }
                } //END IF NOT EMPTY QUERY
            }
        } catch (Exception ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        } 
    }
    
    private Map<Constraint, Double> computeRelaxationProbabilities(Query q) {
        Map<Constraint, Double> probs = new HashMap<>();
        int t;
        Query q1;
        double sum = 0.0, prob;
        for (Constraint c : q.getConstraints()) {
            q1 = (Query) q.clone();
            q1.relax(c);
            t = Utilities.toBooleanQuery(q1);
            prob = pref.compute(q, t);
            sum += prob;
            probs.put(c, prob);
        }
        //Normalize
        for (Constraint c : probs.keySet()) {
            probs.put(c, sum != 0? probs.get(c)/sum : 1.0);
        }
        return probs;
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
        SimpleNode sn;
        String shape = "";
        Node n;

        queue.add(root);
        roots.add(index);
        while (!queue.isEmpty()) {
            n = queue.poll();
            rindex = roots.poll();
            shape = "circle";
            for (Node child : n.getChildren()) {
                sn = (SimpleNode)child;
                edges.add(String.format(PROB_EDGE, rindex, ++index, sn.getRelaxation(), sn.getProbability()));
                queue.add(child);
                roots.add(index);
            }
            nodes.put(rindex, String.format(NODE, rindex, rindex, n.getCost() >= 0 ? String.format("\\nc=%.3f", n.getCost()) : "", shape, "white"));
        }
    }

    /**
     * Compute costs in a bottom-up iterative fashion traversing the tree in pre-order
     * and updating the costs, using <code>updatetCost<code> function from the
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
                    currentNode.setCost(0);
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
     * This computes the cost of a node, depending on the cost of the subtrees
     * if the node is a <code>ChoiceNode</code> the cost is (c(yes) + 1)*p_yes +
     * (c(no) + 1)*p_no, if it is a <code>RelaxationNode</code> the cost is
     * max (c[q']), where q' is a direct subquery of q.
     * @param n The node to update the cost.
     * @throws it.unitn.disi.db.queryrelaxation.tree.TreeException
     * @see ChoiceNode
     * @see RelaxationNode
     */
    public void updateCost(Node n) throws TreeException {//A: takes the average of the children costs
        double avg = 0;
        avg = n.getChildren().stream()
                .map((child) -> ((SimpleNode)child).getProbability() * (1 + child.getCost()))
                .reduce(avg, (accumulator, _item) -> accumulator + _item);
        n.setCost(avg); //Add plus one since we need an interaction step at least
    }

    /**
     * Time spent in computations (in milliseconds)
     * @return The time expressed in milliseconds
     */
    @Override
    public long getTime() {
        return (long)(time.getElapsedTimeMillis()/root.getCost());
        //return (long)((double)time/(expectedHeight/(double)paths.length));
    }


    /**
     * A method to represent the actual tree in a string encoded in Graphviz DOT language
     * @return A string representing the tree
     * @see <a href="http://www.graphviz.org/">Graphviz</a>
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Set<String> tree = new LinkedHashSet<String>();
        Map<Integer, String> costs = new LinkedHashMap<Integer, String>();

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
        for (String path : paths) {
            retval.add(path.trim());
        }
        return retval;
    }
    
    /*
     * Recursive function to produce all the paths separated by a \n
     */
    protected String printPaths(Node n, String acc, boolean optimal) {
        if (n instanceof RelaxationNode) {
            if (n.isLeaf()) {
//                try {
//                    List<Tuple> results = db.submitQuery(n.query);
//                    for (Tuple t : results) {
//                        t.toString()
//                    }
//                } catch (Exception ex) {}
                return acc + "\n";
            } else {
                String result = "";
                for (Node child : n.getChildren()) {
                    if (!optimal || n.getCost() == child.getCost()) { //Take the paths with the cost of the root equal to the cost of the parent - i.e. Opt
                        result += printPaths(child, acc, optimal);
                    }
                }
                return result;
            }            
        } else {
            //ChoiceNode cannot be a leaf
            ChoiceNode cn = (ChoiceNode)n;
//            return printPaths(cn.getYesNode(), acc + cn.getConstraint().getAttributeName() + "|" + cn.getYesProbability() +  "|", optimal) +
//            printPaths(cn.getNoNode(), acc + cn.getConstraint().getAttributeName() + "|" + cn.getNoProbability() + "|", optimal);
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
                    if (n.getCost() == child.getCost()) { //Take the paths with the cost of the root equal to the cost of the parent
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

    public Set<String> getNonEmptyQueries() {
        return nonEmptyQueries;
    }
    
    @Override
    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public static void main(String[] args) {
        BufferedReader testReader = null;
        BufferedWriter br = null;
        String line; 
        String[] splittedLine; 
        Query q;
        BooleanMockConnector db; 
        QueryRefinementTree tree;
        File f = new File(args[0]); 
        System.out.println(f.getAbsoluteFile().getParent());
        
        try {
            testReader = new BufferedReader(new FileReader(f));
            br = new BufferedWriter(new FileWriter(args[1]));
            
            //Extract ipfs and dbs
            while ((line = testReader.readLine()) != null) {
                line = line.trim();
                if (line.length() != 0) {
                    splittedLine = line.split("\t");
                    q = EmptyQueryGeneration.stringToQuery(splittedLine[2]);
                    db = new BooleanMockConnector(f.getAbsoluteFile().getParent()+ File.separator + splittedLine[0]);
                    db.connect();
                    tree = new QueryRefinementTree(q);
                    tree.setDb(db);
                    tree.setPrior(new IPFPrior(db, f.getAbsoluteFile().getParent() + File.separator + splittedLine[1], q));
                    tree.setPref(new IdfFunction(db));
                    tree.setVerbose(false);
                    tree.materialize(true);
                    System.out.println(tree.nonEmptyQueries);
                    for (String neq : tree.nonEmptyQueries) {
                        br.write(neq + "|");
                    }
                    br.newLine();
                    //for (Constraint c : q.getConstraints()) {
                        //query += mapping.get(attMap.get(Integer.parseInt(c.getAttributeName()))) + ", ";
                        //query += c.getAttributeName() + ",";
                    //}
                    //query = query.substring(0, query.length() - 2);
                    //queries.add(query);
                    System.out.printf("Processing query: %s\ndb: %s\nIPF: %s\n", q.toString(), splittedLine[0], splittedLine[1]);
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(QueryRefinementTree.class.getName()).log(Level.SEVERE, "An error occurred in reading the test File, message follows", ex);
        } finally {
            if (testReader != null) {
                try {
                    testReader.close();
                } catch (IOException ex) {
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    Logger.getLogger(QueryRefinementTree.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    @Override
    protected boolean optimalityCondition(Node father, Node child) {
        return false; 
    }

    @Override
    protected boolean isMarked(Node n) {
        return false; 
    }
    
}
