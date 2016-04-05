/*
 * Copyright (C) 2014 Davide Mottin <mottin@disi.unitn.eu>
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

package it.unitn.disi.db.queryrelaxation.tree.topk;

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.tree.ChoiceNode;
import it.unitn.disi.db.queryrelaxation.tree.Node;
import it.unitn.disi.db.queryrelaxation.tree.PruningTree;
import it.unitn.disi.db.queryrelaxation.tree.TreeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Pruning Tree with top-k branches.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TopKPruningTree extends PruningTree {
    protected int k; 
    private final RankingFunction lbRanking; 
    private final RankingFunction ubRanking;
    
    
    public TopKPruningTree(Query query, int cardinality, TreeType type, int k, boolean biased) {
        super(query, cardinality, type);
        lbRanking = new RankingFunction(true);
        ubRanking = new RankingFunction(false);
        this.k = k; 
        this.biased = biased;
    }
    
        
    private class RankingFunction implements Comparator<Node> {
        private boolean lower; 

        public RankingFunction(boolean lower) {
            this.lower = lower;
        }
        
        
        @Override
        public int compare(Node o1, Node o2) {
            Pair<Double, Double> o1Bounds = bounds.get(o1); 
            Pair<Double, Double> o2Bounds = bounds.get(o2); 
            if (o1Bounds == null || o2Bounds == null) {
                System.err.printf("Error: bounds have not been computed yet for %s or %s", o1, o2);
                return 0; 
            }
            if (lower) {
                if (o1Bounds.getFirst() < o2Bounds.getFirst()) {
                    return -1; 
                } else if (o1Bounds.getFirst() > o2Bounds.getFirst()) {
                    return 1;
                }
            } else {
                if (o1Bounds.getSecond() < o2Bounds.getSecond()) {
                    return -1; 
                } else if (o1Bounds.getSecond() > o2Bounds.getSecond()) {
                    return 1;
                }
            }
            return 0; 
        }
        
    }

    @Override
    public void materialize(boolean computeCosts) throws TreeException {
        if (k < 1) {
            throw new TreeException("k cannot be < 1");
        }
        super.materialize(computeCosts); 
    }
    

    /*
     * Prune method changes because we have to consider k nodes.
    */
    @Override
    protected void prune(LinkedList<Node> queue) {
        LinkedList<Node> tree = new LinkedList<>();
        Pair<Double, Double> bound; 
        double kthBound; 
        int count; 
        List<Node> siblings;
        List<Node> candidateSiblings; 
        Node n, sibling;
        
        tree.add(root);
        while (!tree.isEmpty()) { //Explore all the nodes
            n = tree.poll();
            
            siblings = n.getSiblings();
            //If the size of the siblings plus the node <= k then none of them can be pruned
            siblings.add(n);//Add the node to the siblings
            if (siblings.size() + 1 > k) { 
                Collections.sort(siblings, type.isMaximize()? ubRanking : lbRanking);
                candidateSiblings = new ArrayList<>(siblings);
                Collections.sort(candidateSiblings, type.isMaximize() ? lbRanking : ubRanking);                
               
                //Get the kth element in terms of upper/lower bound
                //finding the best subset s of siblings of size k
                bound = null;
                bound = bounds.get(candidateSiblings.get(type.isMaximize()? candidateSiblings.size() - k : k - 1)); 
                kthBound = type.isMaximize()? bound.getFirst() : bound.getSecond();                
//                for (Node sib : candidateSiblings) {
//                    System.out.printf("%s", bounds.get(sib));
//                }
//                System.out.println("");
//                System.out.println("kthBound: " + kthBound);

                count = candidateSiblings.size(); 
                for (int i = 0; i < siblings.size() && count > k; i++) {
                    sibling = siblings.get(type.isMaximize() ? i : siblings.size() - i - 1);

                     //If lb > kth ub, then prune                     
                    if (!type.isMaximize() && bounds.get(sibling).getFirst() > kthBound && sibling instanceof ChoiceNode) {
                        marked.add(sibling);
                        count--;
                    }
                    if (type.isMaximize() && bounds.get(sibling).getSecond() < kthBound && sibling instanceof ChoiceNode) {
                        marked.add(sibling);
                        count--;
                    }                                       
                }//END FOR
                if (this.biased && n instanceof ChoiceNode) {
                    for (int i = candidateSiblings.size() - 1; i >= 0; --i) {
                        boolean propagate = false;
                        HashSet<Constraint> changedConstraints = new HashSet<>();
                        sibling = candidateSiblings.get(i);
                        bound = (Pair)this.bounds.get(sibling);
                        if (this.marked.contains(sibling)) continue;
                        for (int j = i - 1; j >= 0; --j) {
                            Node otherSibling = candidateSiblings.get(j);
                            if (this.marked.contains(otherSibling) || (!this.type.isMaximize() || (Double)((Pair)this.bounds.get(otherSibling)).getFirst() <= (Double)bound.getSecond()) && (this.type.isMaximize() || (Double)((Pair)this.bounds.get(otherSibling)).getSecond() >= (Double)bound.getFirst())) continue;
                            boolean changed = sibling.getQuery().setHard(((ChoiceNode)otherSibling).getConstraint());
                            boolean bl = propagate = propagate || changed;
                            if (!changed) continue;
                            changedConstraints.add(((ChoiceNode)otherSibling).getConstraint());
                        }
                        if (!propagate) continue;
                        this.propagateQuery(sibling, changedConstraints);
                    }
                }
            } //END IF
            for (Node sib : siblings) {
                if (!sib.getChildren().isEmpty()) {
                    tree.add(sib.getChildren().iterator().next());
                }
                //If the father is marked than so are the chilren
                if (marked.contains(sib.getFather())) {
                    marked.add(sib);
                }
            }
       }//END WHILE
    }

    private void propagateQuery(Node node, Set<Constraint> changedConstraints) {
        assert (node instanceof ChoiceNode);
        node.getChildren().stream().filter(n -> !this.marked.contains(n)).forEach(n -> {
            changedConstraints.stream().forEach(constr -> {
                n.getQuery().setHard(constr);
            }
            );
            n.getChildren().stream().filter(child -> changedConstraints.contains(((ChoiceNode)child).getConstraint())).forEach(child -> {
                this.marked.add(child);
            }
            );
        }
        );
    }
    
    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }
}
