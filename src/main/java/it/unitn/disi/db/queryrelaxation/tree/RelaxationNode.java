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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to represent a node to relax which are the possible choices among all the
 * (soft) constraints. A relaxation node can be also associated to the answer set.
 * @author Davide Mottin
 * @see ChoiceNode
 */
public class RelaxationNode extends Node implements Cloneable, Serializable {
    /*
     * This is the map with the edge labels and the choice nodes
     */
    private final Map<String, ChoiceNode> relaxations = new LinkedHashMap<>();
    /*
    * Caching children for optimization purpose
    */
    private List<Node> children; 
    /*
     * If the relaxation does not lead to a valid dataset the node is empty
     */
    private boolean empty = true;

    /**
     * Construct a relaxation node on top of query q
     * @param q The query of the relaxation node. 
     */
    public RelaxationNode(Query q) {
        children = null;
        query = q;
    }

    /**
     * Add a node (child) to this node labelling the edge. Label cannot be null
     * and two different nodes cannot have the same labels.
     * @param label The label of the edge to be added
     * @param node The node to be added having that particular label
     * @return return the previous ChoiceNode having the same label or null if none.
     * @throws NullPointerException if the label is null
     */
    public ChoiceNode addNode(String label, ChoiceNode node)
            throws NullPointerException
    {
        children = null;
        return relaxations.put(label, node);
    }

    /**
     * Prune one subtree from the node
     * @param label The label of the node to be pruned
     * @return Return the pruned node if it has been found, null otherwise
     */
    public ChoiceNode prune(String label) {
        children = null;
        return relaxations.remove(label);
    }

    /**
     * Return the <code>List</code> of children <code>Node</code> associated to this node
     * @return The <code>List</code> of chlidre of this node
     */
    @Override
    public List<Node> getChildren() {
        if (children == null) {
            children = new ArrayList<>(relaxations.values()); 
        }
        return children;
    }

    /**
     * Return the number of vald relaxations for this relaxation node
     * @return the number of relaxations (i.e. children of a node)
     */
    @Override
    public int getChildrenNumber() {
        return relaxations.size();
    }
    
    

    @Override
    public String toString() {
        return "RelaxationNode{" + query + '}';
    }

    /**
     * Remove a child from the node
     * @param n The child <code>Node</code> to be removed
     * @return true if the operation has been completed false otherwise
     */
    @Override
    public boolean removeChild(Node n) {
        boolean outcome = false;
        Set<String> keys = relaxations.keySet();
        children = null; 
        for (String key : keys)
            if (n == relaxations.get(key)) {
                relaxations.remove(key);
                outcome = true;
            }
        return outcome;
    }

    /**
     * A node is empty when the corresponding query does not produce any result
     * @param empty Set the value of empty property
     */
    public void setEmpty(boolean empty) {
        this.empty = empty;
    }

    /**
     * A node is empty when the corresponding query does not produce any result
     * @return true if the node is empty, false otherwise
     */    
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Returns the map of relaxations. Deprecated because it can somehow modify
     * the internal node representation
     * @return A <code>Map</code> containing the labels and the children <code>ChoiceNode</code>s.
     */
    @Deprecated
    public Map<String, ChoiceNode> getRelaxations() {
        return relaxations;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        RelaxationNode rn = new RelaxationNode((Query)this.query.clone()); 
        rn.setEmpty(this.empty);
        rn.setBuckets(this.buckets);
        return rn; 
    }
}
