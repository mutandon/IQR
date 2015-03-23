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
 * Interface to represent the preference function <code>pref</code> as described
 * in the paper
 * @author Davide Mottin
 */
public interface PreferenceFunction {
    /**
     * This function takes in input the query and a tuple and computes the
     * probability that a user likes the tuple given the query
     * @param q The input query (relaxed or not) of the user
     * @param tuple The tuple for which you want to compute the preference of the user
     * @see Tuple
     * @see Query
     * @return Probability of user's preference 
     */
    public double compute(Query q, int tuple);
}
