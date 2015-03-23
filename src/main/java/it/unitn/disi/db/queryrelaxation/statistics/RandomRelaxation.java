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
package it.unitn.disi.db.queryrelaxation.statistics;

import java.util.List;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import java.util.Random;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;

/**
 *
 * @author Alice Marascu
 */
@Deprecated
public class RandomRelaxation {

    private List<Constraint> constraints;
    protected DatabaseConnector db = null;

    public RandomRelaxation(List<Constraint> constraints, DatabaseConnector db) {
        this.constraints = constraints;
        this.db = db;

    }

    public void randomRelaxation() throws ConnectionException {
        // relax the constraints following a random order of attributes relaxation
        int randomConstToRelax = 0;
        boolean noValidRelaxationFound = true;
        Random rand = new Random();
        Query newQuery;
        //check query entirely

        if (db == null) {
            System.out.println("Empty db.");
            System.exit(1);
        };

        while (noValidRelaxationFound && constraints.size() != 0) {
            //randomConstToRelax = rand.nextInt((constraints.size()-1+1) - 0) + 0;
            randomConstToRelax = rand.nextInt((constraints.size()));
            constraints.remove(randomConstToRelax);//removes one constraint
            newQuery = new Query(this.constraints);
            if (db.submitQuery(newQuery).length != 0) {
                System.out.println("A: valid query for " + newQuery);
                noValidRelaxationFound = false;
            } else {
                System.out.println("empty query for " + newQuery);
            };
        }

    }
}
