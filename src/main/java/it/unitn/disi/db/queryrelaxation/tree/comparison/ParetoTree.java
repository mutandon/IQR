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
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.PairSecondComparator;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import it.unitn.disi.db.queryrelaxation.tree.ChoiceNode;
import it.unitn.disi.db.queryrelaxation.tree.Node;
import it.unitn.disi.db.queryrelaxation.tree.PruningTree;
import it.unitn.disi.db.queryrelaxation.tree.RelaxationNode;
import it.unitn.disi.db.queryrelaxation.tree.TreeException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * This is a copy of Interactive Minimum Failing with our optimizations on the 
 * nodes
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ParetoTree extends PruningTree {
    /*
     * A ranked list of attribute names
    */
    private List<String> attributeRanking; 

    
    
    public ParetoTree(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type);
    }
    
    private void computeAttributeRanking(Query q) {
        attributeRanking = new ArrayList<>(); 
        List<Pair<String,Double>> orderedAttributes = new ArrayList<>(); 
        
        q.getConstraints().forEach(cons -> {
            Query qp = new Query();
            qp.addConstraint(cons);
            orderedAttributes.add(new Pair<>(cons.getAttributeName(), pref.compute(q, Utilities.toBooleanQuery(qp))));});
        orderedAttributes.stream()
                .sorted(new PairSecondComparator())
                .forEach((p) -> attributeRanking.add(p.getFirst()));
    }


    @Override
    protected void buildIteratively() throws TreeException {
        //BEGIN-DECLARATIONS
        RelaxationNode rn;
        ChoiceNode cn;
        LinkedList<Node> queue = new LinkedList<>();
        Node n;
        Collection<String> constraints; 

        bounds = new HashMap<>();
        marked = new HashSet<>();
        actualLevel = 1.0;
        nodes++;
        queue.add(root);
        computeAttributeRanking(query);

        //END-DECLARATIONS


        try {
            if (!db.isConnected()) {
                db.connect();
            }
            switch (type) {
                case MIN_EFFORT:
                    bounds.put(root, new Pair<>(1.0, query.size()));
                    break;
                case PREFERRED:
                    bounds.put(root, new Pair<>(0.0, pref.compute(query, Utilities.toBooleanQuery(query))));
                    break;
                default: 
                    throw new TreeException("Unsopported tree type");
            }

            while (!queue.isEmpty()) {
                n = queue.poll();

                if (!n.getQuery().getConstraints().isEmpty() && !n.getQuery().allHardConstraints()) {
                    if (!marked.contains(n)) {
                        if (n instanceof RelaxationNode) {
                            //END-DAVIDE-MOD (Modified also the condition below)
                            //DAVIDE: 14/07/2014 kept for memory
                            if (((RelaxationNode) n).isEmpty()) {
                                constraints = MinimumFailingUtils.minimalConflicts(n.getQuery(), attributeRanking, db);
                                //debug("Min conflicts: %s", constraints);
                                for (String constr : constraints) {
                                    cn = new ChoiceNode();
                                    cn.setFather(n);
                                    cn.setConstraint(new Constraint(constr, true));
                                    cn.setQuery((Query) n.getQuery().clone());
                                    ((RelaxationNode) n).addNode(constr, cn);
                                    queue.add(cn);
                                    bounds.put(cn, new Pair<>(query.size(), query.size()));
                                    if (!marked.contains(n)) {
                                        nodes++;
                                    }
                                }                                
                            }
                        } else if (n instanceof ChoiceNode) {
                            //Build 'yes' node
                            cn = (ChoiceNode) n;
                            queue.add(constructRelaxationNodes(cn, true));
                            //Build 'no' node
                            rn = constructRelaxationNodes(cn, false);
                            cn.setNoNode(1 - cn.getYesProbability(), rn);

                            queue.add(rn);
                            if (!marked.contains(n)) {
                                nodes += 2;
                                relaxationNodes += 2;
                            }
                            //End of the level - Top of the queue is a relaxation node
                            if (queue.peek() instanceof RelaxationNode) {
                                //Update and prune
                                ++actualLevel;
                                update(queue);
                                prune(queue);
                            }
                        }
                    }
                } //END IF NOT EMPTY QUERY
            }
        } catch (ConnectionException | NullPointerException ex) {
            throw new TreeException("Wrong way to build the model, please check", ex);
        }
    }   
}
