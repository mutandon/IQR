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
package it.unitn.disi.db.queryrelaxation.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a query, that is a set of constraints on the data
 * @author Davide Mottin
 * @see Constraint
 */
 public class Query implements Cloneable, Serializable {
    /*
     * List of all constraints to express the query
     */
    private ArrayList<Constraint> constraints;
    /**
     * List of all negated constraints
     */
    private ArrayList<Constraint> negations;

    /**
     * Construct an empty query which is the one without any constraint
     */
    public Query() {
        constraints = new ArrayList<Constraint>();
        negations = new ArrayList<Constraint>();
    }

    /**
     * Construct a query specifying a list of constraints
     * @param constraints A list of constraints for the query
     */
    public Query(List<Constraint> constraints) {
        this();
        this.constraints.addAll(constraints);
    }

    /**
     * Check if this query is a subquery of another one, so check if all the constraints
     * of this query are in query q. The two queries can be the same.
     * @param q The supposed super-query
     * @return true if this is a subquery of q, false otherwise
     */
    public boolean isSubquery(Query q) {
        return q.constraints.containsAll(constraints);
    }

    /**
     * Realaxing a constraint means remove the constraint from the query. If it is
     * not possible to do so the method returns false.
     * @param c The constraint to be relaxed
     * @return true if the constraint can be relaxed, false otherwise
     */
    //Remove c from the query
    public boolean relax(Constraint c) {
        negations.add(c.negate());
        return constraints.remove(c);
    }

    /**
     * Add a constraints to the query, this means probably reducing the number of
     * tuples in the result set
     * @param c The constraint to be added
     * @return true if the constraint has been added to the query, false otherwise
     */
    public boolean addConstraint(Constraint c) {
        return constraints.add(c);
    }

    /**
     * Returns the whole query, that is a list of constraints
     * @return A list of constraints
     */
    public List<Constraint> getConstraints() {
        return (List<Constraint>) constraints.clone();
    }

    /**
     * Return the list of relaxed (i.e. negated) constraints
     * @return The list of relaxed constraints
     */
    public List<Constraint> negatedConstraints() {
        return (List<Constraint>) negations.clone();
    }

    /**
     * Return true if all the constraints are hard constraints.
     * @return True if all the constraints of the query are hard, false otherwise
     */
    public boolean allHardConstraints() {
        for (int i = 0; i < constraints.size(); i++) {
            if (!constraints.get(i).isHard()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return contraints and negations (i.e. the original constraints of the query)
     * @return The contraints and the negations together
     */
    public List<Constraint> constraintsAndNegations() {
        List<Constraint> all = (List<Constraint>) constraints.clone();
        all.addAll(negations);

        return all;
    }

    /**
     * Check if the input query is equal to this one, comparing the list of constraints
     * and negations
     * @param obj The input query to be compared
     * @return true if obj is equal to this, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Query other = (Query) obj;
        if (this.constraints != other.constraints && (this.constraints == null || !this.constraints.equals(other.constraints))) {
            return false;
        }
        if (this.negations != other.negations && (this.negations == null || !this.negations.equals(other.negations))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + (this.constraints != null ? this.constraints.hashCode() : 0);
        hash = 79 * hash + (this.negations != null ? this.negations.hashCode() : 0);
        return hash;
    }

    @Override
    public Object clone() {
        List<Constraint> newConstraints = new ArrayList<Constraint>(constraints.size());
        for (int i = 0; i < constraints.size(); i++) {
            newConstraints.add((Constraint) constraints.get(i).clone());
        }
        Query qu = new Query(newConstraints);

        qu.negations = new ArrayList<Constraint>(negations.size());
        for (int i = 0; i < negations.size(); i++) {
            qu.negations.add((Constraint) negations.get(i).clone());
        }

        return qu;
    }

    @Override
    public String toString() {
        return "Query: constraints{" + constraints + '}' + "; negations{" + negations + "}";
    }

    /**
     * Return the size of the query (without negations)
     * @return The actual size of the query 
     */
    public double size() {
        return constraints.size();
    }

    /**
     * Remove the contraint with index i from the query
     * @param i
     * @return 
     */
    public boolean remove(int i) {
        if (i < 0 || i > constraints.size() - 1) {
            return false;
        }
        constraints.remove(i);
        return true;
    }

    /** 
     * Returns the number of hard constraints of the query
     * @return The number of hard constraints
     */
    public List<Constraint> getHardConstraints() {
        List<Constraint> hardConstraints = new ArrayList<Constraint>();
        for (int i = 0; i < constraints.size(); i++) {
            if (constraints.get(i).isHard()) {
                hardConstraints.add((Constraint)constraints.get(i).clone());
            }
        }
        return hardConstraints;
    }
}
