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

package it.unitn.disi.db.queryrelaxation.exceptions;

import java.util.Formatter;

/**
 * Defines a generic exception that uses formatter in order to have a more expressive
 * exceptions.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class GenericException extends Exception {    
    
    /**
     * Constructs an instance of an exception propoagating an exception
     * @param cause The exception to be rethrown
     */
    public GenericException(Throwable cause) {
        super(cause);
    }
    
    /**
     * Constructs an instance of an exception with the specified detail message.
     * @param msg The output message as a format string.
     * @param params The parameters of a format string
     * @see Formatter
     */
    public GenericException(String msg, Object... params) {
        super(String.format(msg, params));
    }
    
    /**
     * Constructs an instance of an exception propoagating an exception with 
     * an error message
     * @param message The outptu message as a format string
     * @param cause The exception to be rethrown
     * @param params The parameters of a format string
     * @see Formatter
     */
    public GenericException(String message, Throwable cause, Object... params) {
        super(String.format(message, params), cause);
    }
}
