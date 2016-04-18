/*
 * Copyright (C) 2016 Davide Mottin <mottin@disi.unitn.eu>
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
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package it.unitn.disi.db.queryrelaxation.tree.comparison;

import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements the methods in Jannach paper used as subroutine in the minimal failing
 * trees
 * 
 * [1] Jannach, Dietmar. "Techniques for fast query relaxation in content-based 
 * recommender systems." KI 2006: Advances in Artificial Intelligence. 
 * Springer Berlin Heidelberg, 2007. 49-63.
 * 
 * @see InteractiveMinimumFailing
 * @see ParetoTree
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class MinimumFailingUtils {
    private MinimumFailingUtils() {}
    
    
    /*
     * Implement the mfsQI in Figure 8
    */
    private static List<String> mfsQI(List<String> backgroundAtoms, List<String> failingQueryAtoms, DatabaseConnector db) 
            throws ConnectionException 
    {
        List<Constraint> queryConstraints = new ArrayList<>();
        List<String> c1, c2, d1, d2, bgUC1, bgUD1, d1UD2; 
        Query q;
        
        backgroundAtoms.stream().forEach((atom) -> { queryConstraints.add(new Constraint(atom, true)); });
        q = new Query(queryConstraints);
        
        if (failingQueryAtoms.isEmpty() || db.submitQuery(q).length == 0) {
            return new ArrayList<>(); 
        }
        if (failingQueryAtoms.size() == 1) {
            return failingQueryAtoms;
        }
        
        c1 = failingQueryAtoms.subList(0, failingQueryAtoms.size()/2);
        c2 = failingQueryAtoms.subList(failingQueryAtoms.size()/2, failingQueryAtoms.size());
        
        bgUC1 = new ArrayList<>(backgroundAtoms);
        bgUC1.addAll(c1);
        bgUD1 = new ArrayList<>(backgroundAtoms);
        d1 = mfsQI(bgUC1, c2, db);
        bgUD1.addAll(d1);
        d2 = mfsQI(bgUD1, c1, db);
        d1UD2 = new ArrayList<>(d1);
        d1UD2.addAll(d2);
        
        return d1UD2; 
    }

    /**
     * Implement the modified QuicXPlain method with the same name in the paper. 
     * The names are as close as possible as Fig. 8. 
     * 
     * @param orderedAttributes The list of attributes ordered by preference
     * @param db The input database
     * @return The preferred conflict of Q
     * @throws ConnectionException In case of problems with the database
     */
    public static Collection<String> mfsQX(List<String> orderedAttributes, DatabaseConnector db) 
            throws ConnectionException 
    {
        return mfsQI(new ArrayList<>(), orderedAttributes, db);
    }
    
    /**
     * Find the minimal conflicts (i.e. the attributes to relax). 
     * @param q An empty-answer query
     * @param attributeRanking The list of attributes ordered by preference
     * @param db The input database
     * @return The set of minimal conflicts
     * @throws ConnectionException In case of problems with the database
     */
    public static Collection<String> minimalConflicts(Query q, List<String> attributeRanking, DatabaseConnector db) 
            throws ConnectionException 
    {
        Set<String> queryAttributeNames = new HashSet<>();
        List<String> rankedAttributes; 
        
        q.getConstraints().stream()
                .filter((constr) -> !constr.isHard())
                .forEach((constr) -> queryAttributeNames.add(constr.getAttributeName()));
        
        rankedAttributes = new ArrayList<>();
        attributeRanking.stream().filter((att) -> queryAttributeNames.contains(att)).forEach((att) -> rankedAttributes.add(att));
        return mfsQX(rankedAttributes, db);
    }
}
