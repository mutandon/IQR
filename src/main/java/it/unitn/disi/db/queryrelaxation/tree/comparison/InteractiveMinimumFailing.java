/*
 * The MIT License
 *
 * Copyright 2015 Davide Mottin <mottin@disi.unitn.eu>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package it.unitn.disi.db.queryrelaxation.tree.comparison;

import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.PairSecondComparator;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import it.unitn.disi.db.queryrelaxation.tree.ChoiceNode;
import it.unitn.disi.db.queryrelaxation.tree.Node;
import it.unitn.disi.db.queryrelaxation.tree.OptimalRelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationNode;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationTree;
import it.unitn.disi.db.queryrelaxation.tree.TreeException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class adapts and implements the Interactive method based on minimum failing queries
 * presented in [1]
 * 
 * [1] Jannach, Dietmar. "Techniques for fast query relaxation in content-based 
 * recommender systems." KI 2006: Advances in Artificial Intelligence. 
 * Springer Berlin Heidelberg, 2007. 49-63.
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class InteractiveMinimumFailing extends OptimalRelaxationTree {
    /*
     * A ranked list of attribute names
    */
    private List<String> attributeRanking; 
    /*
     * Set the backtrack choice as in the original Algorithm
    */
    private boolean backtrack = true; 
    

    public InteractiveMinimumFailing(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type);
    }

    public InteractiveMinimumFailing(Query q) {
        super(q);
    }
    
    private void computeAttributeRanking(Query q) {
        attributeRanking = new ArrayList<>(); 
        List<Pair<String,Double>> orderedAttributes = new ArrayList<>(); 
        
        q.getConstraints().forEach(c -> {
            Query qp = new Query();
            qp.addConstraint(c);
            orderedAttributes.add(new Pair<>(c.getAttributeName(), pref.compute(q, Utilities.toBooleanQuery(qp))));});
        orderedAttributes.stream()
                .sorted(new PairSecondComparator())
                .forEach((p) -> attributeRanking.add(p.getFirst()));
    }
    
    
    /**
     * Implement the modified QuicXPlain method with the same name in the paper. 
     * The names are as close as possible as Fig. 8. 
     * 
     * @return The preferred conflict of Q
     */
    private List<String> mfsQI(List<String> backgroundAtoms, List<String> failingQueryAtoms) 
            throws ConnectionException 
    {
        List<Constraint> queryConstraints = new ArrayList<>();
        List<String> c1, c2, d1, d2, bgUC1, bgUD1, d1UD2; 
        Query q;
        
        backgroundAtoms.stream().forEach((atom) -> { queryConstraints.add(new Constraint(atom, true)); });
        q = new Query(queryConstraints);
        
        if (failingQueryAtoms.isEmpty() || db.submitQuery(q).length == 0) {
            return new ArrayList<>(); 
        }
        if (failingQueryAtoms.size() == 1) {
            return failingQueryAtoms;
        }
        
        c1 = failingQueryAtoms.subList(0, failingQueryAtoms.size()/2);
        c2 = failingQueryAtoms.subList(failingQueryAtoms.size()/2, failingQueryAtoms.size());
        
        bgUC1 = new ArrayList<>(backgroundAtoms);
        bgUC1.addAll(c1);
        bgUD1 = new ArrayList<>(backgroundAtoms);
        d1 = mfsQI(bgUC1, c2);
        bgUD1.addAll(d1);
        d2 = mfsQI(bgUD1, c1);
        d1UD2 = new ArrayList<>(d1);
        d1UD2.addAll(d2);
        
        return d1UD2; 
    }

    /*
     * Implement the mfsQX in Figure 8
    */
    private Collection<String> mfsQX(List<String> orderedAttributes) 
            throws ConnectionException 
    {
        return mfsQI(new ArrayList<>(), orderedAttributes);
    }
    
    @Override
    public void materialize(boolean computeCosts) throws TreeException {
        time.reset();
        time.start();
        buildIteratively();
        
        if (computeCosts) {
            computeCosts();
        }
        time.stop();
    }

    @Override
    protected void buildIteratively() throws TreeException {        
        RelaxationNode rn;
        ChoiceNode cn;
        Query q;
        Double probability;
        LinkedList<Node> queue = new LinkedList<>();
        
        
        //boolean leaf;
        Collection<String> constraints; 
        int[] results; 
        Node n;
        queue.add(root);
        nodes = 1;
        relaxationNodes = 1;
        computeAttributeRanking(query);
        try {            
            if (!db.isConnected()) {
                db.connect();
            }
            //debug(Utilities.printLattice(query, db));

            while (!queue.isEmpty()) {
                n = queue.poll();
                //No further relaxations or only hard constraints. 
                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) { 
                    if (n instanceof RelaxationNode) {
                        if (((RelaxationNode) n).isEmpty()) {
                            constraints = minimalConflicts(n.getQuery());
                            //debug("Min conflicts: %s", constraints);
                            for (String constr : constraints) {
                                cn = new ChoiceNode();
                                cn.setFather(n);
                                cn.setConstraint(new Constraint(constr, true));
                                cn.setQuery((Query) n.getQuery().clone());
                                ((RelaxationNode) n).addNode(constr, cn);
                                queue.add(cn);
                                nodes++;
                            }
                        }
                    } 
                    else if (n instanceof ChoiceNode) {
                        //Build yes node
                        q = (Query) n.getQuery().clone();
                        q.relax(((ChoiceNode) n).getConstraint());
                        rn = new RelaxationNode(q);

                        probability = computeYesProbability(q, (RelaxationNode) n.getFather());
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
                        q = new Query();
                        for (Constraint c : rn.getQuery().getConstraints()) {
                            if (c.isHard()) {
                                q.addConstraint(c);
                            }
                        }
                        q.negatedConstraints().addAll(rn.getQuery().negatedConstraints());
                        //If the query gives us some result the node is not empty
                        results = db.submitQuery(q);
                        //If it is >= cardinality the search continues, otherwise it stops
                        //If !backtrack stops
                        rn.setEmpty(results.length >= cardinality && backtrack); 
                        //DAVIDE-MOD-END
                        ((ChoiceNode) n).setNoNode(1 - probability, rn);
                        //((ChoiceNode) n).setNoNode(computeNoProbabilitySecondVersion(q, (RelaxationNode) n.father), rn);
                        queue.add(rn);
                        relaxationNodes++;
                        nodes++;
                    }
                } //END IF NOT EMPTY QUERY
            }
        } catch (ConnectionException | NullPointerException ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        } 
    }
    
    private Collection<String> minimalConflicts(Query q) 
            throws ConnectionException 
    {
        Set<String> queryAttributeNames = new HashSet<>();
        List<String> rankedAttributes; 
        
        q.getConstraints().stream()
                .filter((constr) -> !constr.isHard())
                .forEach((constr) -> queryAttributeNames.add(constr.getAttributeName()));
        
        rankedAttributes = new ArrayList<>();
        attributeRanking.stream().filter((att) -> queryAttributeNames.contains(att)).forEach((att) -> rankedAttributes.add(att));
        return mfsQX(rankedAttributes);
    }
    
    /*
     * Even probability? Is this correct? 
     */
    @Override
    protected double computeNoProbability(Query q1, RelaxationNode parent) throws ConnectionException {
        return 0.5;
    }

    @Override
    protected double computeYesProbability(Query q1, RelaxationNode parent) throws ConnectionException {
        return 0.5;
    }

    /**
     * Given no preference function and user model we model the cost as the average
     * @param n The input node
     * @throws TreeException 
     */
    @Override
    public void updateCost(Node n) throws TreeException {
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());
        } else if (n instanceof RelaxationNode) {
            double sum; 
            sum = n.getChildren().stream()
                    .map((child) -> child.getCost())
                    .reduce(0., (accumulator, _item) -> accumulator + _item);
            n.setCost(sum/n.getChildrenNumber());
            if (n == root) 
            {
                info("Root cost: %f", sum);
            }

        }
    }

    @Override
    protected void computeLeafCost(RelaxationNode n) throws ConnectionException {
        assert n.isLeaf() : "Node must be a leaf"; 
        Pair<int[],double[]> resultSet;
        //double max = 0; 
        double cost = 0;
        Query q = new Query(n.getQuery().getConstraints());
        
        if (!n.isEmpty()) {
            switch (type) {
                case MIN_EFFORT : 
                    break;
                case PREFERRED : 
                    double preference = 0;
                    resultSet = db.resultsAndBenefits(n.getQuery()); 
                    if (resultSet.getFirst().length > cardinality) {
                        preference = pref.compute(q, resultSet.getFirst()[0]);
                    }
                    n.setCost(preference);
                    //n.setCost(max);                
                    break;
                default:
                    throw new AssertionError("Wrong type");
            }
        }
        n.setCost(cost);
    }

    public boolean isBacktrack() {
        return backtrack;
    }

    public void setBacktrack(boolean backtrack) {
        this.backtrack = backtrack;
    }
    
//    @Override
//    public RelaxationTree optimalTree(TreeType tt) throws TreeException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    protected boolean isMarked(Node n) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
    
    
}
