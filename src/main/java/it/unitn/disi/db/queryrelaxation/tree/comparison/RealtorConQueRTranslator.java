/*
 * IQR (Interactive Query Relaxation) Library
 * Copyright (C) 2013  Davide Mottin (mottin@disi.unitn.eu
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

package it.unitn.disi.db.queryrelaxation.tree.comparison;

import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Pair;
import it.unitn.disi.db.queryrelaxation.model.PairFirstComparator;
import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.BooleanMockConnector;
import it.unitn.disi.db.queryrelaxation.model.functions.IdfFunction;
import it.unitn.disi.db.queryrelaxation.statistics.EmptyQueryGeneration;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Produce Why Not queries to be used as input of [1]
 * <p>[1] Quoc Trung Tran, Chee-Yong Chan: How to ConQueR why-not questions. 
 * SIGMOD Conference 2010: 15-26</p>
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class RealtorConQueRTranslator {
    
    //private List<String> queries;
    private static List<String> mapping;
    private static final Integer[] DOUBLE_MAPPING = {15,16,19,22,29,30,31,32,34,35,36,37,38,39,40,41,42,43};
    //private static final String[] NAMES = {"address","state","zipcode","mslid","price","type","area","county","subdivision","year","bedroom ","bath","fullbath","approarea","story","Single house","Living area","Dining area","Kitchen area","Heating features","Air conditioning","intfeatures","External features","extconst","applot","schooldist","bedroomcount","bathroomcount","fence","Deck","Swimming Pool","Waterfront","Smoke Detector","Garage","Parking","Automatic Gates","Porch","Playground","Community Clubhouse","Trees","Courtyard","Sidewalk","Cul-de-Sac","Landscape"};
    private static final String[] NAMES = {"address","state","zipcode","mslid","price","type","area","county","subdivision","year","bedroom ","bath","fullbath","approarea","story","single_house","living_area","dining_area","kitchen_area","heating_features","air_conditioning","intfeatures","external_features","extconst","applot","schooldist","bedroomcount","bathroomcount","fence","deck","swimming_pool","waterfront","smoke_detector","garage","parking","automatic_gates","porch","playground","community_clubhouse","trees","courtyard","sidewalk","cul_de_sac","landscape"};
    private static List<Map<Integer,Integer>> attributeMaps;
    
    private RealtorConQueRTranslator() {}
    
    public static void translate(String queryFile, String outputDir, int nTuples, boolean isInt) {
        BufferedReader testReader = null;
        BufferedWriter writer = null;
        String line = null;
        String query, cons;
        String attributes; 
        String[] splittedLine, indeces;
        Query q; 
        List<Constraint> constraints;
        int[] tuples;
        List<Pair<Integer,Double>> orderedTuples; 
        //String basePath = new File(queryFile).getParent();
        Map<Integer,Integer> attMap;
        int type, index;
        BooleanMockConnector db;
        Constraint c;
        StringBuilder sb, where, select;
        PreferenceFunction pref;
        int count = 0; 
        String table = isInt? "houseint" : "house";
        Pair<Integer, Double> tuple; 
        
        //ueries = new ArrayList<String>();
        //mapping = new ArrayList<String>();
        attributeMaps = new ArrayList<Map<Integer,Integer>>();

        try {
        
            //testReader.close();
            testReader = new BufferedReader(new FileReader(queryFile));
            //Extract ipfs and dbs
            while ((line = testReader.readLine()) != null) {
                line = line.trim();
                if (line.length() != 0) {
                    count++;
                    attributes = "";
                    query = "";
                    sb = new StringBuilder();
                    where = new StringBuilder();
                    select = new StringBuilder();
                    attMap = new HashMap<Integer,Integer>();
                    query = "";
                    splittedLine = line.split("\t");
                    q = EmptyQueryGeneration.stringToQuery(splittedLine[2]);
                    db = new BooleanMockConnector(splittedLine[0]);
                    db.connect();
                    pref = new IdfFunction(db);
                    
                    //Get the whole database
                    tuples = db.submitQuery(new Query());
                    orderedTuples = new ArrayList<>();
                    for (int t : tuples) {
                        orderedTuples.add(new Pair<>(t, pref.compute(q, t)));
                    }
                    Collections.sort(orderedTuples, new Comparator<Pair<Integer, Double>>() {

                        @Override
                        public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                            if (o1.getSecond() == o2.getSecond())
                                return 0; 
                            else if (o1.getSecond() > o2.getSecond())
                                return -1; 
                            return 1; 
                        }
                    });
                    
                    
                    indeces = splittedLine[0].substring(splittedLine[0].lastIndexOf("att_") + 4,splittedLine[0].lastIndexOf(".")).split("_");
                    type = Integer.parseInt(splittedLine[3]);
                    for (int i = 0; i < indeces.length; i++) {
                        index = Integer.parseInt(indeces[i]);
                        if (type == 1) {
                            index = DOUBLE_MAPPING[index];
                        }
                        attributes += NAMES[index] + ",";
                        attMap.put(i, index);
                    }
                    /*
                    _QUERY
                    SELECT distinct player.name 
                    FROM player, player_regular_season
                    WHERE player.pid = player_regular_season.pid AND player_regular_season.year >= 2000 AND player_regular_season.pts > 2300 ;

                    _WN
                    N|2|Rick Barry|Wilt Chamberlain
                    */
                    attributeMaps.add(attMap);
                    
                    constraints = q.getConstraints();
                    for (int i = 0; i < constraints.size(); i++) {
                        c = constraints.get(i);
                        cons = NAMES[attMap.get(Integer.parseInt(c.getAttributeName()))];
                        select.append(table).append(".").append(cons).append(i >= constraints.size() - 1 ? "" : ", ");
                        where.append(table).append(".").append(cons).append(" = 1").append(i >= constraints.size() - 1 ? "" : " AND ");
                        query += NAMES[attMap.get(Integer.parseInt(c.getAttributeName()))] + ", ";
                        //query += c.getAttributeName() + ",";
                    }
                    
                    sb.append("_QUERY\nSELECT ").append(select);
                    sb.append("\nFROM ").append(table).append("\n");
                    sb.append("WHERE ").append(where);
                    sb.append(" ;\n\n");
                    
                    sb.append("_WN\n");
                    sb.append("N|").append(nTuples).append("|");
                    //Tuple t; 
                    for (int i = 0; i < nTuples; i++) {
                        tuple = orderedTuples.get(i);
                        constraints = q.getConstraints();
                        for (int j = 0; j < constraints.size(); j++) {
                            c = constraints.get(j);
                            sb.append((tuple.getFirst() & (1 << Integer.parseInt(c.getAttributeName()))) > 0 ? "1" : "0");
                            if (j < constraints.size() - 1) {
                                sb.append("_");
                            }
                        }
                        sb.append(i < nTuples - 1? "|" : "");  
                    }
                    writer = new BufferedWriter (new FileWriter(outputDir + File.separator + (isInt? "rqi" : "rq") + count + ".sql"));
                    writer.append(sb);
                    writer.close();
                    
                    query = query.substring(0, query.length() - 2);
                    //queries.add(query);
                    System.out.printf("Processed query: %s\ndb attributes: %s\n", query, attributes);
                    System.out.printf("SQL query:\n%s\n\n", sb.toString());                    
                    //System.out.printf("");
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(RealtorConQueRTranslator.class.getName()).log(Level.SEVERE, "An error occurred in reading the test File, message follows", ex);
        } finally {
            if (testReader != null) {
                try {
                    testReader.close();
                } catch (IOException ex) {
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ex) {
                }
            }
        }
    }


    public String getAttribute(String name, int query) {
        Integer attIndex;
        String attribute = null;
        try {
            attIndex = Integer.parseInt(name);
            attribute = mapping.get(attributeMaps.get(query).get(attIndex));
        } catch (Exception ex) {
            Logger.getLogger(RealtorConQueRTranslator.class.getName()).log(Level.SEVERE, "An error occurred while converting the attribute", ex);
        }
        return attribute;
    }
    
    public List<String> getAttributes(int query) {
        List<String> attributes = null;
        Map<Integer,Integer> attMap;
        try {
            attributes = new ArrayList<String>();
            attMap = attributeMaps.get(query);
            for (Integer key : attMap.keySet()) {
                attributes.add(mapping.get(attMap.get(key)));
            }
        
        } catch (Exception ex) {
            Logger.getLogger(RealtorConQueRTranslator.class.getName()).log(Level.SEVERE, "An error occurred while converting the attribute", ex);
        }
        return attributes;
                
    }
    
    public static void main(String[] args) {
        RealtorConQueRTranslator.translate("InputData/dataset_A", "/Users/mutandon/NetBeansProjects/ConQueR/test_query", 5, true);
    }
    
}
