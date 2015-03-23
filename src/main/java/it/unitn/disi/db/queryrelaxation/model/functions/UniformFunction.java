/*
 * IQR (Interactive Query Relaxation) Library
 * Copyright (C) 2012  Davide Mottin (mottin@disi.unitn.eu
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

import it.unitn.disi.db.queryrelaxation.model.PreferenceFunction;
import it.unitn.disi.db.queryrelaxation.model.Query;

/**
 * An implementation of a uniform preference function (returns a constant)
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class UniformFunction implements PreferenceFunction {

    public double compute(Query q, int t) {
        return 1.0;
    }
    
}
