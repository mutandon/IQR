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
 */
package it.unitn.disi.db.queryrelaxation.statistics;

import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Constraint;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * This (static) class contains some useful functions to be used
 *
 * @author Davide Mottin
 */
public class Utilities {

    private Utilities() {
    }

    public static int hammingDistance(int w1, int w2) {
        return bitCount(w1 ^ w2);
    }

    /** 
     * Counts number of 1 bits in an efficient way
     * @param u the input number
     * @return the number of 1 bits
     */
    public static int bitCount(int u) {
        int uCount;
        uCount = u - ((u >> 1) & 033333333333)
                - ((u >> 2) & 011111111111);
        return ((uCount + (uCount >> 3))
                & 030707070707) % 63;
    }

    public static void stringToFile(String file, String content) throws IOException {
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter(file));
            br.append(content);
        } catch (IOException ex) {
            throw new IOException(ex);
        } finally {
            try {
                br.close();
            } catch (Exception ex) {
            }
        }
    }

    public static List<File> findFilesOnSubdirs(String path, Pattern filter) {
        File root = new File(path);
        File[] list = root.listFiles();
        List<File> validFiles = new ArrayList<File>();

        if (list.length == 0) {
            return Collections.emptyList();
        }
        for (File f : list) {
            if (f.isDirectory()) {
                validFiles.addAll(findFilesOnSubdirs(f.getAbsolutePath(), filter));
            } else {
                if (filter.matcher(f.getName()).matches()) {
                    validFiles.add(f);
                }
            }
        }
        return validFiles;
    }

    public static String join(Collection<?> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<?> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }
    
    public static int toBooleanQuery(Query q) {
        int query = 0;
        String name;
        for (Constraint cons : q.getConstraints()) {
            name = cons.getAttributeName();
            query += (1 << Integer.parseInt(name));
        }//END FOR
        return query;
    }

    public static String matrixToString(double[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            sb.append("[").append(i).append("] => [");
            for (int j = 0; j < matrix[i].length; j++) {
                sb.append(matrix[i][j]).append(" ");
            }
            sb.append("]\n");
        }
        return sb.toString();
    }
    
    
    public static String printLattice(Query q, DatabaseConnector db) 
            throws ConnectionException 
    {
        List<Query> lattice = queryLattice(q); 
        StringBuilder sb = new StringBuilder(); 
        int lastSize = (int) q.size();
        
        if (!db.isConnected()) {
            db.connect();
        }
        lattice.sort((q1,q2) -> (int)q2.size() - (int)q1.size());
        for (Query q1 : lattice) {
            if (q1.size() != lastSize) {
                sb.append("\n");
                lastSize = (int) q1.size();
            }
            if (db.submitQuery(q1).length == 0) {
                sb.append("X");
            }
            sb.append(q1.getConstraints().toString()).append(" ");
        }
        return sb.toString(); 
    }
    
    
    public static List<Query> queryLattice(Query q) {
        if (q.size() == 0) {
            return new ArrayList<>(Collections.singletonList(new Query())); 
        }
        
        List<Query> lattice, newLattice; 
        
        Constraint first = q.getConstraints().get(0);
        Query qSub = (Query) q.clone();
        qSub.remove(0);
        lattice = queryLattice(qSub);
        
        newLattice = new ArrayList<>(); 
        newLattice.addAll(lattice);
        for (Query q1: lattice) {
            qSub = (Query) q1.clone();
            qSub.addConstraint(first);
            newLattice.add(qSub);
        }   
        return newLattice; 
    }
           
}
