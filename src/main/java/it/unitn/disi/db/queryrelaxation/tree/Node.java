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

import it.unitn.disi.db.queryrelaxation.model.Query;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class represents a general node of the relaxation tree.
 * @author Davide Mottin
 */
public abstract class Node implements Serializable {
    /*
     * This cost is a function of the height of the three and it is used to prune
     * the children. 
     */
    protected double cost = -1.0;
    /*
     * The father of this node
     */
    protected Node father;
    /*
     * The query is needed in order to know if the answer set is empty or not
     */
    protected Query query;
    /*
     * Buckets of the probability distribution associated
     * 1st line: buckets limits (+1, from current to next=one bucket), first bucket from O to 1
     * 2nd line: prob values
     */
    protected double[][] buckets;  //A: added

    /**
     * Return the cost associated to this node.
     * @return The cost
     */
    public double getCost() {
        return cost;
    }

    /**
     * Set the cost of the node
     * @param cost A double representing the cost
     */
    public void setCost(double cost) {
        this.cost = cost;
    }

    /**
     * Return the father node, if any
     * @return The father node
     */
    public Node getFather() {
        return father;
    }

    /**
     * Get the buckets approximating the cost distribution by means of histograms 
     * used by the CDR-based algorithms
     * @return the buckets
     */
    public double[][] getBuckets(){  
        return this.buckets;
    }

    /** 
     * Set th buckets used by CDR-based algorithms
     * @param newBuckets 
     */
    public void setBuckets(double[][] newBuckets){
        this.buckets = newBuckets;
    }

    /**
     * Return the siblings, i.e. children of your father apart from this,
     * associated with this node
     * @return A <code>List</code> of siblings node
     */
    public ArrayList<Node> getSiblings() {
        ArrayList<Node> siblings = new ArrayList<>();
        if (father != null) {
            List<Node> children = father.getChildren();
            for (Node n : children)
                if (n != this)
                    siblings.add(n);
        }
        return siblings;
    }

    /**
     * Set the father node to traverse the tree also in the opposite direction
     * @param father The father node
     */
    /* This function maybe at some point useful */
    public void setFather(Node father) {
        this.father = father;
    }

    /**
     * Returns the <code>Query</code> associated to this node
     * @return The query associated to this node
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Set the <code>Query</code> of the node to the new value
     * @param query The query to be associated to this node.
     */
    public void setQuery(Query query) {
        this.query = query;
    }

    /**
     * Return true if the node is a leaf. A node is a leaf if all the children are
     * null
     * @return true if this node is a leaf, false otherwise
     */
    public boolean isLeaf() {
        for (Node child : getChildren()) {
            if (child != null)
                return false;
        }
        return true;
    }

    /**
     * Remove a child from the node
     * @param n The child <code>Node</code> to be removed
     * @return true if the operation has been completed false otherwise
     */
    public abstract boolean removeChild(Node n);

    /**
     * Return the <code>List</code> of children <code>Node</code> associated to this node
     * @return The <code>List</code> of chlidren of this node
     */
    public abstract List<Node> getChildren();

    /**
     * Return the number of children <code>Node</code> associated to this node.
     * @return The number of children for this node. 
     */
    public int getChildrenNumber() {
        return getChildren().size();
    }
    
    /**
     * Represents the node into a string
     * @return The string representation of the node
     */
    @Override
    public abstract String toString();

    /**
     * Return the (estimated) distance from the root of the tree of the node, computed
     * using the information in the query. 
     * @return The estimated distance from the root of the tree. 
     */
    public int getLevel(){
        return this.query.negatedConstraints().size() + this.query.getHardConstraints().size();
    }
}
