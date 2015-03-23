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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
/**
 * A class representing a choice node that is a node with only two chldren, one
 * for the yes answer and one for the no answer. Yes and No are the possible
 * choices that represent the case in whihc the user accepts or refuse a relaxation
 * @author Davide Mottin
 */
public class ChoiceNode extends Node implements Cloneable, Serializable {
    /*
     * The constraint to be relaxed
     */
    private Constraint constraint;
    /*
     * The yes node along with the yes answer probability
     */
    private Pair<Double, RelaxationNode> yesNode;
    /*
     * The no node along with the no answer probability
     */
    private Pair<Double, RelaxationNode> noNode;
    /*
     * The cached list of children (for performance issue)
    */
    private List<Node> children;
    
    
    /**
     * The choice node associated to this
     */
    public ChoiceNode() {
        this(null, null);
    }
     /**
      * Construct a node setting initial probailities to 0.5
      * @param yes The yes node of this choice node
      * @param no The no node of this choice node
      */
    public ChoiceNode(RelaxationNode yes, RelaxationNode no) {
        children = null; 
        yesNode = new Pair<>(.5, yes);
        noNode = new Pair<>(.5, no);

    }

    public void setNoNode(double probability, RelaxationNode noNode) {
        children = null; 
        this.noNode = new Pair<>(probability, noNode);
    }

    public void setYesNode(double probability, RelaxationNode yesNode) {
        children = null;
        this.yesNode = new Pair<>(probability, yesNode);
    }

    public double getYesProbability() {
        return yesNode.getFirst();
    }

    public double getNoProbability() {
        return noNode.getFirst();
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public void setConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    public RelaxationNode getYesNode() {
        return yesNode.getSecond();
    }

    public RelaxationNode getNoNode() {
        return noNode.getSecond();
    }


    @Override
    public List<Node> getChildren() {
        if (children == null) {
            children = new ArrayList<>();
            children.add(yesNode.getSecond());
            children.add(noNode.getSecond());
        }
        return children;
    }

    /**
     * Return the number of children that is always two for a choice node
     * @return the number of children (2) 
     */
    @Override
    public int getChildrenNumber() {
        return 2; //To change body of generated methods, choose Tools | Templates.
    }
    
    

    @Override
    public String toString() {
        return "ChoiceNode{" + "yesNode=" + yesNode.getFirst() + ",noNode=" + noNode.getFirst() + '}';
    }

    @Override
    public boolean removeChild(Node n) {
        boolean outcome = false;
        children = null;
        if (n == yesNode.getSecond()) {
            yesNode.setSecond(null);
            outcome = true;
        } else if (n == noNode.getSecond()) {
            noNode.setSecond(null);
            outcome = true;
        }
        return outcome;
    }

    /**
     * Perform a shallow copy of the node, all children and the father are null.
     * @return A shallow copy of the choice node
     */
    @Override
    protected Object clone() { 
        ChoiceNode c = new ChoiceNode();
        c.constraint = (Constraint) constraint.clone();
        c.noNode = new Pair<>(getNoProbability(), null);
        c.yesNode = new Pair<>(getYesProbability(), null);
        c.query = (Query) query.clone();
        c.buckets = buckets;
        c.father = null;
        return c;
    }
}
