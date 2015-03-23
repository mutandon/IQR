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
package it.unitn.disi.db.queryrelaxation.commands;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import it.unitn.disi.db.queryrelaxation.exceptions.ConnectionException;
import it.unitn.disi.db.queryrelaxation.model.Query;
import it.unitn.disi.db.queryrelaxation.model.data.BooleanMockConnector;
import it.unitn.disi.db.queryrelaxation.model.data.DatabaseConnector;
import it.unitn.disi.db.queryrelaxation.statistics.EmptyQueryGeneration;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generates queries used in the experiments
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class GenerateQueries extends Command {
    private String dbFolder;
    private String queryFile; 
    private int minQuerySize; 
    private int maxQuerySize;
    
    @Override
    protected void execute() throws ExecutionException {        
        String pathToDb, pathToIPF;
        File oneDir; 
        String[] dirs; 
        String[] files; 
        File directory = new File(dbFolder);
        DatabaseConnector db; 
        Query q; 
        
        if (directory == null) {
            throw new ExecutionException("Error in opening the directory.");
        }
        if (directory.list() == null) {
            throw new ExecutionException("Error: the directory" + directory + " does not contains anything.");
        }
        dirs = directory.list();

        try (BufferedWriter saveWriter = new BufferedWriter(new FileWriter(queryFile))) {
            for (int i_i = 0; i_i < dirs.length; i_i++) {
                oneDir = new File(directory.getPath(), dirs[i_i]);

                if (oneDir.isDirectory()) {
                    files = oneDir.list(); //will have a db file and an ipf file
                    if (files.length != 2) {
                        error("there are more than 2 files (db, ipf) in this directory" + files);
                        throw new ExecutionException("More than two files in the directory");
                    }
                    if (files[0].contains("forIPF")) {
                        pathToIPF = new File(oneDir.getAbsoluteFile(), files[0]).getAbsolutePath();
                        pathToDb = new File(oneDir.getAbsoluteFile(), files[1]).getAbsolutePath();
                    } else {
                        pathToIPF = new File(oneDir.getAbsoluteFile(), files[1]).getAbsolutePath();
                        pathToDb = new File(oneDir.getAbsoluteFile(), files[0]).getAbsolutePath();
                    }

                    //process files
                    info("Processing the files:\n ipf: " + pathToIPF
                            + "\n and db:" + pathToDb);

                    //generate queris
                    db = new BooleanMockConnector(pathToDb);
                    db.connect();

                    //test profiler
                    List<Query> genQueries = null;
                    genQueries = (new EmptyQueryGeneration()).generateQueries(db, minQuerySize, maxQuerySize, 3, 10, 10, db.getAttributeNumber());// yahoo cluster
                    if (queryFile != null) {
                        for (Query query : genQueries) {
                            saveWriter.append(String.format("%s\t%s\t%s\n", pathToDb, pathToIPF, EmptyQueryGeneration.queryToString(query)));
                        }
                        saveWriter.flush();
                    }
                    for (int iii = 0; iii < genQueries.size(); iii++) {
                        q = genQueries.get(iii);
                        info("Start processing the query:" + q);
                        if (db.submitQuery(q).length != 0) {
                            error("The query is not empty.");
                            throw new ExecutionException("The query is not empty");
                        }
                    }
                }//END IF ONE DIR
            }//END FOR
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GenerateQueries.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {

        } catch (NumberFormatException ex) {
            Logger.getLogger(GenerateQueries.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ConnectionException ex) {
            Logger.getLogger(GenerateQueries.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(GenerateQueries.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected String commandDescription() {
        return "Generates queries used in the experiments";
    }

    @CommandInput(
        consoleFormat = "-db",
        defaultValue = "",
        mandatory = true,
        description = "folder containing the database to be used")
    public void setDbFolder(String dbFolder) {
        this.dbFolder = dbFolder;
    }

    @CommandInput(
        consoleFormat = "-o",
        defaultValue = "queries",
        mandatory = false,
        description = "file containing the queries")
    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    @CommandInput(
            consoleFormat = "-m",
            defaultValue = "3",
            mandatory = false,
            description = "minimum query size to be processed")
    public void setMinQuerySize(int minQuerySize) {
        this.minQuerySize = minQuerySize;
    }

    @CommandInput(
            consoleFormat = "-M",
            defaultValue = "7",
            mandatory = false,
            description = "maximum query size to be processed")
    public void setMaxQuerySize(int maxQuerySize) {
        this.maxQuerySize = maxQuerySize;
    }    
}
