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

package it.unitn.disi.db.queryrelaxation.exceptions;

/**
 * Class to represent exceptions that occur in the database
 * @author Davide Mottin
 */
public class DataException extends Exception {

    /**
     * Creates a new instance of <code>DataException</code> without detail message.
     */
    public DataException() {
    }

    /**
     * Constructs an instance of <code>DataException</code> propoagating an exception
     * @param cause The exception to be rethrown
     */
    public DataException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an instance of <code>DataException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public DataException(String msg) {
        super(msg);
    }
}
