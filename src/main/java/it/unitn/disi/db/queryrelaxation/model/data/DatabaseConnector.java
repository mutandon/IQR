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

package it.unitn.disi.db.queryrelaxation.model.data;

import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.Query;

/**
 * Abstracts the connection to the database in order to be able to connect to
 * different kind of sources (DAO pattern)
 * @author Davide Mottin
 */
public interface DatabaseConnector {

    /**
     * Connect to the database if possible
     * @return true if the connection holds, false otherwise
     * @throws ConnectionException if an error happens
     */
    public boolean connect() throws ConnectionException;

    /**
     * Return true if the database connection is alive, false othewise. 
     * @return Return the state of connections
     */
    public boolean isConnected();
    /**
     * Close the connection to the database, if possible
     * @return true if the connection has been close, false otherwise
     * @throws ConnectionException if something unexpected happens
     */
    public boolean close() throws ConnectionException;

    /**
     * Submits the query to the database and finds out the results
     * @param q The <code>Query</code> to be submitted
     * @return A list of tuples that is the result set of the querys
     * @throws ConnectionException if something unexpected happens
     */
    public int[] submitQuery(Query q) throws ConnectionException;

    /**
     * Submits a query and returns results and their benefits
     * @param q The <code>Query</code> to be submitted
     * @return A <code>Pair</code> containing the results of the query and their benefits
     * @throws ConnectionException if something unexpected happens
     */
    public Pair<int[],double[]> resultsAndBenefits(Query q) throws ConnectionException; 

    /**
     * Computes the tuple space (i.e. all the possible combination) constrained by
     * a particular query q. You can specify if the tuple space must take into
     * account also negative attribute-values or not.
     * @param q The input query
     * @param negatives A boolean to specify if taking into account negated cosntraints
     * @return The list of tuples in the tuple space
     * @throws ConnectionException 
     */
    //public int[] queryTupleSpace(Query q, boolean negatives) throws ConnectionException;

    /**
     * Return all the attribute names in the dataset
     * @return An array of attribute names
     * @throws ConnectionException If something unexpected happens
     */
    public String[] getAttributeNames() throws ConnectionException;

    /** 
     * Return the number of attributes in the database
     * @return the nunber of attributes
     */
    public int getAttributeNumber();
    
    //TODO: this help us knowing the set of possible values for an attribute, for
    //now only consider boolean databases
    //public Object[] getPossibleValues(String attributeName) throws ConnectionException;

    /**
     * Returns the size of the database, i.e. the total number of tuples
     * @return The size of the database
     * @throws ConnectionException If something unexpected happens
     */
    public int size() throws ConnectionException;

    /**
     * Count the number of tuples having a particular values for a particular attribute
     * index
     * @param attIndex The index of attribute you want to consider
     * @param value The value of the attribute you need to check
     * @return The number of matching tuples
     * @throws ConnectionException If something unexpected happens
     */
    public int count(int attIndex, Object value) throws ConnectionException;
    
    /**
     * Compute the idf (inverse document frequency) for the particular attribute
     * index and a particular value. The inverse document frequency returns the log-
     * reciprocal ratio between the number of tuples having a value over the size
     * of the database
     * @param attIndex The index of the attribute to consider
     * @param value The value of the attribute you need to check
     * @return The number of matching tuples
     * @throws ConnectionException  If something unexpected happens
     */
    public double idf(int attIndex, Object value) throws ConnectionException;
    
    /**
     * Get the minimum benefit or value of the database
     * @return The minimum benefit
     * @throws ConnectionException If something unexpected happens
     */
    public double getMinBenefit() throws ConnectionException; 
    
    /**
     * Get the minimum benefit or value of the database
     * @return The maximum benefit
     * @throws ConnectionException If something unexpected happens
     */
    public double getMaxBenefit() throws ConnectionException;
    
    /**
     * Given a query find the min-max benefit of that query result
     * @param q The input query to be performed 
     * @return The min benefit associated to the corresponding result set
     * @throws ConnectionException 
     */
    public Pair<Double, Double> getMinMaxBenefit(Query q) throws ConnectionException; 
}
