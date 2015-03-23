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

/**
 * Represents an attribute in a database, each database should have a set of attributes
 * And an attribute can get only discrete values (this is our assumption)
 * @author Davide Mottin
 */
public class Field<T extends Comparable> {
    /*
     * The name of attribute that ideally represents a field in a database
     */
    protected String attributeName;
    /*
     * The value of the attribute, this can be a complex object
     */
    protected T value;


    public Field(String name, T value) {
        this.attributeName = name;
        this.value = value;
    }

    /**
     * Returns the name of the constraint
     * @return the attribute name
     */
    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Set the name of the constraint
     * @param attributeName The (attribute) name of the constraint
     */
    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    /**
     * Returns the value associated to this constraint
     * @return the value of the constraint
     */
    public T getValue() {
        return value;
    }

    /**
     * Set the value of the constraint
     * @param value The value of the constraint
     */
    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public Object clone() {
        Field newConstraint = new Field(attributeName, value);
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
        final Field<T> other = (Field<T>) obj;
        if ((this.attributeName == null) ? (other.attributeName != null) : !this.attributeName.equals(other.attributeName)) {
            return false;
        }
        if (this.value != other.value && (this.value == null || !this.value.equals(other.value))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 43 * hash + (this.attributeName != null ? this.attributeName.hashCode() : 0);
        hash = 43 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }

}
