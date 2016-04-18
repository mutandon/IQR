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

/**
 * This class represents a constraint which is a pair name,value for a particular
 * attribute of the database. The value must be comaprable in order to match the
 * tuples in the database that  comply with the constraint.
 *
 * <p>The simplest case is a boolean constraint, in which the value can take only
 * two values, true or false.</p>
 *
 * @author Davide Mottin
 */
public class Constraint implements Serializable {
    /*
     * A constraint can be hard or soft whether it can be relaxed or not.
     */
    private boolean hard = false;
    /*
     * Add a comparison function here, looks below
     */
    private ComparisonFunction function;
    /*
     * The name of attribute that ideally represents a field in a database
     */
    protected String attributeName;
    /*
     * The value of the attribute, this can be a complex object
     */
    protected Comparable value;


    /**
     * Construct a soft-constraint, having an attribute name and a value
     * @param attributeName The name of the constraint
     * @param value The value of the constraint
     */
    public Constraint(String attributeName, Comparable value) {
        this.attributeName = attributeName;
        this.value = value;
        hard = false;
        function = ComparisonFunction.EQUAL;
    }


    /**
     * Returns true if the constraint cannot be relaxed (i.e. is an hard constraint)
     * @return true if it is hard, false otherwise
     */
    public boolean isHard() {
        return hard;
    }

    /**
     * Set to true if the constraint cannot be relaxed in the future
     * @param hard a boolean to harden the constraint
     */
    public void setHard(boolean hard) {
        this.hard = hard;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Comparable getValue() {
        return value;
    }

    public void setValue(Comparable value) {
        this.value = value;
    }

    @Override
    public Object clone() {
        Constraint newConstraint = new Constraint(attributeName, value);
        newConstraint.hard = hard;
        newConstraint.function = function;
        return newConstraint;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Constraint other = (Constraint) obj;
        if (this.function != other.function || !this.attributeName.equals(other.attributeName) || !this.value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + (this.function != null ? this.function.hashCode() : 0);
        hash = 79 * hash + (this.attributeName != null ? this.attributeName.hashCode() : 0);
        hash = 79 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        if (hard)
            return String.format("-%s-", attributeName);
        return attributeName;
    }

    /**
     * Return the negation of the constraint (in a boolean setting)
     * @return The negated constraint
     */
    public Constraint negate() {
        if (value instanceof Boolean)
            return new Constraint(attributeName,!((Boolean)value));
        return this;
    }
}
