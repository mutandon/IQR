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

import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;

/**
 * Prior Function can change depending on the definition. The prior is a knowledge
 * about the database, which basically represents the probability that the user
 * believes that the tuple exists in the database
 * @author Davide Mottin
 */
public abstract class Prior {
    protected DatabaseConnector db;

    /**
     * Build a prior using the database db
     * @param db The database to be used
     */
    public Prior(DatabaseConnector db) {
        this.db = db;
    }

    /**
     * Get the prior knowledge of the user (estimates of the beliefs of the user
     * with respect to a tuple)
     * @param tuple Tuple to compute the prior with respect to the database
     * @return The prior knowledge of the user. 
     */
    public abstract double getProbability(int tuple);
}
