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

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Prior;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class computes the Prior using Iterative Proportional Fitting (IPF) method
 * @author Davide Mottin
 * @see <a href="http://en.wikipedia.org/wiki/Iterative_proportional_fitting">IPF</a>
 */
public final class IPFPrior extends Prior {
    private final Map<Integer, Double> PROBS = new HashMap<>();

    int[] indicesToConsider;

    public IPFPrior(DatabaseConnector db, String ipfFilePath, Query query) throws
            ConnectionException, java.io.FileNotFoundException, java.io.IOException, NumberFormatException {
        super(db);
        //1. with optimization
        filterIndicesToConsiderFromIPF(query); //kept in indicesToConsider
        readAndFilterIPFFile(ipfFilePath); //A: reads only the filtered part of the tuple and sums the probs
        //2. without oprimization
        //        readIPFFile(ipfFilePath); //A: old version that reads the entire tuple

    }

    public void filterIndicesToConsiderFromIPF(Query query) {
        this.indicesToConsider = new int[(int) query.size()];
        List<Constraint> constr = query.getConstraints();
        int i = 0;

        try {
            for (Constraint con : constr) {
                this.indicesToConsider[i++] = Integer.parseInt(con.getAttributeName());
            }
        } catch (NumberFormatException e) {
            System.out.println("Error in converting to int the constraint of the query " + query);
            System.exit(1);
        }
    }

    public boolean attToConsider(int value) {

        for (int i = 0; i < this.indicesToConsider.length; i++) {
            if (this.indicesToConsider[i] == value) {
                return true;
            }
        }
        return false;

    }

    public void readAndFilterIPFFile(String ipfFilePath) throws
            java.io.FileNotFoundException, java.io.IOException, NumberFormatException {//A:

        BufferedReader br = new BufferedReader(new FileReader(ipfFilePath));
        String line = "", token = "";
        StringTokenizer st;
        int counter = 0;
        int tuple;

        while ((line = br.readLine()) != null) {
            st = new StringTokenizer(line, " \t");
            counter = 0;
            tuple = 0; 

            while (st.hasMoreTokens()) {
                token = st.nextToken().trim();

                if (st.hasMoreTokens()) {// not the last elem, so not the probab
                    if (attToConsider(counter)) {  //tuple will keep only the considered indices values
                        if (token.equals("1")) {
                            //this.t.addValue(counter + "", true);
                            tuple += 1 << counter;  
                        }
                    }
                    counter++;
                } else {//it's the last elem, so the probab
                    if (PROBS.containsKey(tuple)) {//checks if exists => probs addition
                        PROBS.put(tuple, new Double(token) + PROBS.get(tuple));
                    } else {
                        if (Float.valueOf(token) == 0f) {
                            PROBS.put(tuple, new Double(0.001f));//replace the 0 values with a small value
                        } else {
                            PROBS.put(tuple, new Double(token));
                        }
                    }
                }
            }
        }
        br.close();
    }

//    public void readIPFFile(String ipfFilePath) throws
//            java.io.FileNotFoundException, java.io.IOException, NumberFormatException {//A:
//
//        BufferedReader br = new BufferedReader(new FileReader(ipfFilePath));
//        String line = "", token = "";
//        StringTokenizer st;
//        int counter = 0;
//
//        while ((line = br.readLine()) != null) {
//            st = new StringTokenizer(line, " \t");
//            counter = 0;
//            this.t = new Tuple();
//
//            while (st.hasMoreTokens()) {
//
//                token = st.nextToken().trim();
//
//                if (st.hasMoreTokens()) {// not the last elem, so not the probab
//                    if (token.equals("1")) {
//                        this.t.addValue(counter + "", true);
//                    } else {
//                        this.t.addValue(counter + "", false); //it's zero
//                    }
//                    counter++;
//                } else {//it's the last elem, so the probab
//                    if (Float.valueOf(token) == 0f) {
//                        PROBS.put(this.t, new Double(0.001f));//replace the 0 values with a small value
//                    } else {
//                        PROBS.put(this.t, new Double(token));
//                    }
//                }
//
//            }
//        }
//        br.close();
//    }

    @Override
    public double getProbability(int t) {
        if (!PROBS.containsKey(t)) {
            System.out.println("PROBS does not contain this tuple:" + Integer.toBinaryString(t));
        }
        return PROBS.get(t);
    }

    public Map<Integer, Double> getPROBS() {
        return this.PROBS;
    }
}
