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
package it.unitn.disi.db.queryrelaxation.tree;

import eu.unitn.disi.db.mutilities.LoggableObject;
import eu.unitn.disi.db.mutilities.StopWatch;
import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Prior;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;

/**
 * This class represents a general relaxation tree used in our experiments as 
 * described in the paper. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class RelaxationTree extends LoggableObject {
    protected Query query;
    protected DatabaseConnector db;
    protected PreferenceFunction pref;
    protected Prior prior; 
    protected boolean verbose;
    protected StopWatch time;
    protected int nodes;
    protected int cardinality; 
    protected Node root;
    protected TreeType type;
    
    /**
     * Express the kind of tree you want to realize. The general framework can 
     * be used to optimize several types of tree. 
     */
    public enum TreeType {
        MIN_EFFORT(false), 
        MAX_VALUE_AVG(true), 
        MAX_VALUE_MAX(true), 
        PREFERRED(true);
        
        private final boolean maximize;
        
        TreeType(boolean maximize) {
            this.maximize = maximize;
        }

        public boolean isMaximize() {
            return maximize;
        }
    };
    
    
    public RelaxationTree(Query q, int cardinality, TreeType type) {
        query = q;
        this.cardinality = cardinality; 
        time = new StopWatch(StopWatch.TimeType.CPU);
        this.type = type;
    }
    
    public RelaxationTree(Query q) {
        this(q, 1, TreeType.MIN_EFFORT);
    }
    
    /**
     * Build the whole tree starting by the root using the database, if any.
     * @param computeCosts True if you want to also compute costs, otherwise
     * you can call <code>computeCosts</code> function separately.
     * @throws TreeException If the tree construction generates an error.
     */
    public abstract void materialize(boolean computeCosts) throws TreeException;
    
    /**
     * Compute costs in a bottom-up iterative fashion traversing the tree in pre-order
     * and updating the costs, using <code>updatetCost<code> function from the
     * leaves to the root.
     * @throws TreeException If it is not possible to compute the cost
     */
    public abstract void computeCosts() throws TreeException;
    
    /**
     * Compute the value of the optimial tree (the tree containing only the best
     * path given the objective function) with respect to a different objective 
     * function 
     *  
     * @param tt The objective function to be compared to
     * @return The <code>RelaxationTree</code> that represents the optimal tree
     * @throws TreeException If something wrong happens
    */
    public abstract RelaxationTree optimalTree(TreeType tt) throws TreeException;

    /**
     * Return true if the child node is the one with the optimal cost. 
     * @param father the father node
     * @param child the child node
     * @return true if the child is the optimal
     */
    protected abstract boolean optimalityCondition(Node father, Node child);

    /**
     * Return true if the node n is marked (e.g., pruned) somehow
     * @param n the node to be checked
     * @return true if the node is marked
     */
    protected abstract boolean isMarked(Node n);
    
    /**
     * Compute the expected number of relaxations performed by a user using the 
     * specific tree model
     * @return The expected number of relaxations
     */
    public double expectedRelaxations() {
        return expectedRelaxations(root);
    }
    
    /*
     * Internal method to compute the expected number of relaxations at a
     * certain node
    */
    protected double expectedRelaxations(Node n) {
        if (n.isLeaf()) {
            return n.getQuery().negatedConstraints().size();
        }
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode)n;
            return cn.getYesProbability() * expectedRelaxations(cn.getYesNode()) 
                    + cn.getNoProbability() * expectedRelaxations(cn.getNoNode());
        } else {
            double sum; 
            sum = n.getChildren().stream()
                    .filter((child)  -> optimalityCondition(n, child))
                    .map((child) -> expectedRelaxations(child))
                    .reduce(0.0, (accum, _item) -> accum + _item);
            return sum / n.getChildrenNumber(); 
        }
    }
    
    
    @Override
    public abstract String toString();
    
    public DatabaseConnector getDb() {
        return db;
    }

    public void setDb(DatabaseConnector db) {
        this.db = db;
    }

    public int getNumberOfNodes() {
        return nodes;
    }

    public Prior getPrior() {
        return prior;
    }

    public void setPrior(Prior pr) {
        this.prior = pr;
    }

    public PreferenceFunction getPref() {
        return pref;
    }

    public void setPref(PreferenceFunction pref) {
        this.pref = pref;
    }

    public Query getQuery() {
        return query;
    }

    public long getTime() {
        return time.getElapsedTimeMillis();
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }
    
    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public Node getRoot() {
        return root;
    }

    public void resetTime() {
        time.reset();
    }

    public TreeType getType() {
        return type;
    }
}
