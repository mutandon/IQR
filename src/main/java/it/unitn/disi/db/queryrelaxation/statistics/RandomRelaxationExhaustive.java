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
import java.util.ArrayList;

/**
 *
 * @author Alice Marascu
 */
@Deprecated
public class RandomRelaxationExhaustive {//the greedy algorithm

    private List<Constraint> constraints;
    protected DatabaseConnector db = null;

    public RandomRelaxationExhaustive(List<Constraint> constraints, DatabaseConnector db) {
        this.constraints = constraints;
        this.db = db;
    }

    public void randomRelaxation() throws ConnectionException {
        boolean noValidRelaxationFound = true;
        Random rand = new Random();
        if (db == null) {
            System.out.println("Empty db, exit program.");
            System.exit(1);
        };
        Query newQuery;
        List<Constraint> cloneL;

        while (noValidRelaxationFound && constraints.size() != 0) {
            for (int i = 0; i < constraints.size(); i++) {
                cloneL = new ArrayList<Constraint>(constraints);
                cloneL.remove(i);
                newQuery = new Query(cloneL);

                if (db.submitQuery(newQuery).length != 0) {
                    System.out.println("A: valid query for " + newQuery);
                    noValidRelaxationFound = false;
                    break;

                }
            }
            constraints.remove(rand.nextInt((constraints.size())));
        }
    }
}
