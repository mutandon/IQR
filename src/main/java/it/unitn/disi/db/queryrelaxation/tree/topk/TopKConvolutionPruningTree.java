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

import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.PairSecondComparator;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.statistics.Utilities;
import it.unitn.disi.db.queryrelaxation.tree.ChoiceNode;
import it.unitn.disi.db.queryrelaxation.tree.ConvolutionPruningTree;
import it.unitn.disi.db.queryrelaxation.tree.Node;
import it.unitn.disi.db.queryrelaxation.tree.TreeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Convolution pruning tree (FastCDR) with top-k proposed branches.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TopKConvolutionPruningTree extends ConvolutionPruningTree {
    //Given that I cannot inherit from multiple classes I need to copy/paste the code
    //from TopKPruningTree. It's horrible, I know, don't blame me. 

    protected int k;
    private final RankingFunction lbRanking;
    private final RankingFunction ubRanking;
    /*
     * List containing the ranked branches to be expanded. 
     */
    private LinkedList<Pair<Node, Integer>> expandableBranches;

    private class RankingFunction implements Comparator<Node> {

        private final boolean lower;

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

    public TopKConvolutionPruningTree(Query query, int levelL, int noOfBuckets, int cardinality, TreeType type, int k) {
        super(query, levelL, noOfBuckets, cardinality, type);
        ubRanking = new RankingFunction(false);
        lbRanking = new RankingFunction(true);
        this.k = k;
    }

    @Override
    public void materialize(boolean computeCosts) throws TreeException {
        if (k < 1) {
            throw new TreeException("k cannot be < 1");
        }
        super.materialize(computeCosts);
    }

    /*
     * We have:
     * - uncompleted branches that contains what the algorithm should expand but hasn't already done
     * - unconsidered branches: branches to ignore
     * - expandableRoots: contain the top-k roots to be expanded. 
     * - currentLevel: is the level that we are considering in the expansion
     * - actualLevel: contains the max expanded level for the current root
     */
    @Override
    public void buildIteratively() throws TreeException {
        unconsideredBranches = new HashSet<>();
        expandableBranches = new LinkedList<>();
        numberOfSteps = 1;
        marked = new HashSet<>();
        bounds = new HashMap<>();
        Pair<Node, Integer> nodeLevel;
        currentLevel = level;
        expandableBranches.add(new Pair<>(root, 1));
        nodes++;

        //Now we have everything in expandable roots.
        while (!expandableBranches.isEmpty()) {
            nodeLevel = expandableBranches.poll();
            currentRoot = nodeLevel.getFirst();
            actualLevel = nodeLevel.getSecond();
            if (actualLevel > currentLevel) {
                currentLevel++;
                numberOfSteps++;
            }
            constructByLevel();
            if (currentLevel < query.size() && query.size() > level) {
                if (currentRoot.getChildrenNumber() > k) {
                    computeApproximation();
                } else {
                    for (Node children : currentRoot.getChildren()) {
                        expandableBranches.add(new Pair<>(children, (int) actualLevel));
                    }
                }
            }
        }
    }

    @Override
    protected void computeTopRoots() {
        double maxProb = Double.MAX_VALUE, tmpProb, tmp;
        Node bestRootChild = currentRoot; //can be erased
        PriorityQueue<Pair<Node, Double>> rankedBranches = new PriorityQueue<>((int) query.size(), new PairSecondComparator(true));
        Node branch;
        int count = 0;

        for (Node child1 : currentRoot.getChildren()) {
            if (!marked.contains(child1)) {
                if (verbose) {//Very verbose
                    System.out.println(Utilities.matrixToString(child1.getBuckets()));
                }
                tmpProb = 0;
                //   checkIfBucketsAreAProb(child1.getBuckets());
                for (Node child2 : currentRoot.getChildren()) {
                    //  checkIfBucketsAreAProb(child2.getBuckets());
                    if (child1 != child2) {
                        //compute p(child1 < child2)
                        for (int i = 0; i < noOfBuckets - 1; i++) {
                            tmp = 0;
                            for (int j = i + indexAddendum; j < noOfBuckets - 1; j++) {
                                tmp += child2.getBuckets()[1][j];
                            }
                            tmpProb += tmp * child1.getBuckets()[1][i];
                        }
                    }
                }
                if (type.isMaximize()) {
                    tmpProb = 1 - tmpProb;
                }
                rankedBranches.add(new Pair<>(child1, tmpProb));
                if (verbose) {
                    System.out.printf("Prob: %f - Child: %s - %s\n", tmpProb, child1.toString(), ((ChoiceNode) child1).getYesNode().getQuery());
                }
                if (maxProb < tmpProb || maxProb == Double.MAX_VALUE) { //
                    maxProb = tmpProb;
                    bestRootChild = child1;//can be erase
                }
            }
        }
        if (verbose) {
            System.out.printf("Best root child: %s", ((ChoiceNode) bestRootChild).getYesNode().getQuery());
            //System.out.println("Convolution-Cost of the root is:" + minProb + " for child:" + bestRootChild);
            System.out.println("Convolution-Cost of the root is:" + maxProb + " for child:" + bestRootChild);
        }
        while (!rankedBranches.isEmpty() && count++ < k) {
            branch = rankedBranches.poll().getFirst();
            for (Node children : branch.getChildren()) {
                if (!children.isLeaf()) {
                    expandableBranches.add(new Pair<>(children, (int) actualLevel));
                }
            }
        }
        for (Pair<Node, Double> rankedBranch : rankedBranches) {
            unconsideredBranches.add(rankedBranch.getFirst());
        }
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

        tree.add(getCurrentRoot());
        while (!tree.isEmpty()) { //Explore all the nodes
            n = tree.poll();

            siblings = n.getSiblings();
            //If the size of the siblings plus the node <= k then none of them can be pruned
            siblings.add(n);//Add the node to the siblings
            if (siblings.size() + 1 > k) {
                Collections.sort(siblings, type.isMaximize() ? ubRanking : lbRanking);
                candidateSiblings = new ArrayList<>(siblings);
                Collections.sort(candidateSiblings, type.isMaximize() ? lbRanking : ubRanking);

                //Get the kth element in terms of upper/lower bound
                //finding the best subset s of siblings of size k
                bound = null;
                bound = bounds.get(candidateSiblings.get(type.isMaximize() ? candidateSiblings.size() - k : k - 1));
                kthBound = type.isMaximize() ? bound.getFirst() : bound.getSecond();

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

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

}
