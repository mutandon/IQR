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
package it.unitn.disi.db.queryrelaxation.model.functions;

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;

/**
 * Implements the Normalized Hamming Function to use when you compute the 
 * relaxation tree
 * @author Davide Mottin
 */
public class HammingFunction implements PreferenceFunction {

    /**
     * Return the normalized hamming function as described in the paper, i.e.
     * HammingDistance(q,t)/|q|
     * @param q The query of the user
     * @param t The tuple to compute the distance
     * @return The probability that the user likes the tuple given the query
     */
    @Override
    public double compute(Query q, int t) {
        double distance = 0.0;
        int refValue = 0; 
        
        for (Constraint cons : q.getConstraints()) {
            refValue += 1 << Integer.parseInt(cons.getAttributeName());
        }
        distance += Utilities.bitCount(t & refValue);
        refValue = 0; 
        for (Constraint cons : q.negatedConstraints()) {
            refValue += 1 << Integer.parseInt(cons.getAttributeName());
        }
        distance += Utilities.bitCount((~t) & refValue);        
        return distance / q.size();
    }
}
