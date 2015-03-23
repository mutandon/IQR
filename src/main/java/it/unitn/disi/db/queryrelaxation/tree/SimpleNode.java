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

import it.unitn.disi.db.queryrelaxation.model.Query;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represent a simple node which contains children node of the same type
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class SimpleNode extends Node implements Serializable {
    private List<Node> children;
    private String relaxation;
    private boolean empty = true;
    private double probability = 1.0; //Initally we have Equal likelihood
    
    public SimpleNode(Query q) {
        this(q, "");
    }
    
    public SimpleNode(Query q, String relaxation) {
        query = q;
        children = new ArrayList<Node>();
        this.relaxation = relaxation;
    }
        
    @Override
    public boolean removeChild(Node n) {
        return children.remove(n);
    }

    @Override
    public List<Node> getChildren() {
        return children;
    }
    
    public boolean addChild(Node n) {
        return children.add(n);
    }

    @Override
    public String toString() {
        return "SimpleNode{" + query + '}';
    }

    public String getRelaxation() {
        return relaxation;
    }

    public void setRelaxation(String relaxation) {
        this.relaxation = relaxation;
    }

    public boolean isEmpty() {
        return empty;
    }

    public void setEmpty(boolean empty) {
        this.empty = empty;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }
}
