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
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Greedy relaxation tree with a random choice of non-empty nodes.
 * @author Alice Marascu
 */
public class GreedyRelaxationRandomChoiceTree extends OptimalRelaxationTree {
    private static final int REPETITIONS_TIME = 100;

    public GreedyRelaxationRandomChoiceTree(Query query, int cardinality, TreeType type) {
        super(query, cardinality, type, 1, false);
    }
    
    public GreedyRelaxationRandomChoiceTree(Query query) {
        super(query);
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
    public void updateCost(Node n) throws TreeException {//A: takes the cost of the child that leads to valid answer else left most
        if (n instanceof ChoiceNode) {
            ChoiceNode cn = (ChoiceNode) n;
            cn.setCost((cn.getYesNode().getCost() + c) * cn.getYesProbability() + (cn.getNoNode().getCost() + c) * cn.getNoProbability());
        } else if (n instanceof RelaxationNode) {
            /*          //1. left most choice for the cost in case of no valid answer
            n.setCost(n.getChildren().get(0).getCost());
            try{
            for (Node child : n.getChildren())
            if (!db.submitQuery(child.query).isEmpty()) {
            n.setCost(child.getCost());
            break;
            }
            }catch (ConnectionException ce){System.out.println("exception in db connection from greedy meth");System.exit(1);}
            //end 1
             */

            //2. random choice for the cost in case of no valid answer
            //n.setCost(n.getChildren().get(0).getCost());
            boolean found = false;
            try {
                for (Node child : n.getChildren()) {
                    if (db.submitQuery(child.query).length != 0) {
                        n.setCost(child.getCost());
                        found = true;
                        break;
                    }
                }
                if (!found) {

                    n.setCost(n.getChildren().get((new Random()).nextInt((n.getChildren().size()))).getCost());
                }
            } catch (ConnectionException ce) {
                System.out.println("exception in db connection from greedy meth");
                System.exit(1);
            }
            // end 2


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
        List<Node> children;
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
          //      print = "";
                while (true) {
                    if (n instanceof RelaxationNode) {
                        if (n.isLeaf()) {
                            db.submitQuery(n.query); //Pay anyway
                            break;
                        }
                        children = n.getChildren();
                        Collections.shuffle(children);//Randomize the choice
                        n = children.get(0);
                        for (Node child : children) {
                            if (db.submitQuery(((ChoiceNode)child).getYesNode().query).length != 0) {
            //                    print += ((ChoiceNode)child).getConstraint().getAttributeName() + "|";
                                n = child;
                                break;
                            }
                        }
                    } else {
                        cn = (ChoiceNode) n;
                        if (r.nextDouble() <= cn.getYesProbability()) {
                            n = cn.getYesNode();
              //              print += "yes|";
                        } else {
                            n = cn.getNoNode();
                //            print += "no|";
                        }
                    }
                }
                //System.out.println(print);
                avgTime += System.nanoTime();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return avgTime / (k * 1000000);
    }
    
}
