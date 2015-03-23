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
import it.unitn.disi.db.queryrelaxation.statistics.BenefitGenerator;
import java.io.IOException;

/**
 * Generate synthtetic benefits for a database. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class GenerateBenefits extends Command {
    private String db; 
    
    @Override
    protected void execute() throws ExecutionException {
        try {
            BenefitGenerator.generateBenefits(db);
        } catch (IOException ex) {
            throw new ExecutionException(ex);
        }
    }

    @Override
    protected String commandDescription() {
        return "Generate synthetic benefits for a database";
    }
    
    @CommandInput(
        consoleFormat = "-db",
        defaultValue = "",
        mandatory = true,
        description = "folder containing the database to be used")
    public void setDb(String db) {
        this.db = db;
    }
}
