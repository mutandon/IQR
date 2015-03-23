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

import it.unitn.disi.db.queryrelaxation.model.Prior;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a simple one-dimensional prior, more sofisticated priors use
 * Iterative Proportional Fitting algorithm.
 *
 * Let us suppose the database being a boolean database. This is a first attempt.
 * @author Davide Mottin
 */
public class SimplePrior extends Prior {
    private Map<String, Double> probabilities = new HashMap<>();
    private String[] attributes;

    public SimplePrior(DatabaseConnector db) throws ConnectionException {
        super(db);
        computeModel();
    }

    @Override
    public double getProbability(int t) {
        double prob = 1.0;
        
        for (int i = 0; i < attributes.length; i++) {
            
            if ((t & (1 << i)) == 1)
                prob *= probabilities.get(attributes[i]);
            else
                prob *= (1 - probabilities.get(attributes[i]));
        }
        return prob;
    }

    private void computeModel() throws ConnectionException {
        try {
            db.connect();
            attributes = db.getAttributeNames();
            double size = db.size();
            for (int i = 0; i < attributes.length; i++) {
                probabilities.put(attributes[i], db.count(i, true)/size);
                //THIS is the simple True probability since the database has only boolean values
            }
        } catch (ConnectionException cex) {
            throw new ConnectionException(cex);
        } finally {
            try {
                db.close();
            } catch (Exception e) {
            }
        }
    }
}
