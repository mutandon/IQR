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

package it.unitn.disi.db.queryrelaxation.tree;

import it.unitn.disi.db.queryrelaxation.model.Query;
import java.util.Random;

/**
 * Random algorithm which averages all the costs of the children in the 
 * relaxation nodes
 * @author Alice Marascu
 */
public class RandomRelaxationTree extends OptimalRelaxationTree {
    private static final int REPETITIONS_TIME = 10000;
        
    public RandomRelaxationTree(Query query) {
        super(query);
    }

    public RandomRelaxationTree(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type);
    }
    
    /**
     * This computes the cost of a node, depending on the cost of the subtrees
     * if the node is a <code>ChoiceNode</code> the cost is (c(yes) + 1)*p_yes +
     * (c(no) + 1)*p_no, if it is a <code>RelaxationNode</code> the cost is
     * max (c[q']), where q' is a direct subquery of q.
     * @param n The node to update the cost.
     * @see ChoiceNode
     * @see RelaxationNode
     */
    @Override
    public void updateCost(Node n) throws TreeException {//A: takes the average of the children costs
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());
        } else if (n instanceof RelaxationNode) {
            double avg = 0;
            for (Node child : n.getChildren()) {
                avg += child.getCost();
            }
            if (n.getChildren().isEmpty()) {
                throw new TreeException("the node " + n + " has no child");
            }
            avg = avg / n.getChildren().size();
            n.setCost(avg);
            /*double min = Double.MAX_VALUE;
            for (Node child : n.getChildren()) {
            if (child.getCost() < min) {
            min = child.getCost();
            }
            }
            n.setCost(min);
             *
             */
        }
    }

    @Override 
    public long getTime() {
        return this.getTime(REPETITIONS_TIME);
    }
    
    /**
     * Compute the time in a random choice, repeat k times and average the time.
     * @param k Times to repeat in order to measure the time to perform this method
     * @return average time to perform this method
     */
    public long getTime(int k) {
        Random r = new Random();
        Node n;
        ChoiceNode cn;
        long avgTime = 0;
        //String print;
        try {
            if (!db.isConnected()) {
                db.connect();
            }
            for (int i = 0; i < k; i++) {
                avgTime -= System.nanoTime();
                n = root;
                //print = "";
                while (n != null) {
                    if (n instanceof RelaxationNode) {
                        if (!n.isLeaf()) {
                            n = n.getChildren().get(r.nextInt(n.getChildren().size()));
                //            print += ((ChoiceNode)n).getConstraint().getAttributeName() + "|";
                        } else {
                            db.submitQuery(n.query);//To add this cost in any case
                            break;
                        }
                    } else {
                        cn = (ChoiceNode)n;
                        if (r.nextDouble() <= cn.getYesProbability()) {
                            n = cn.getYesNode();
                //            print += "yes|";
                        } else {
                            n = cn.getNoNode();
                //            print += "yes|";
                        }
                    }
                }
                //System.out.println(print);
                avgTime += System.nanoTime();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return avgTime / (k*1000000);
    }
}
