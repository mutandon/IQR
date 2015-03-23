/*
 * IQR (Interactive Query Relaxation) Library
 * Copyright (C) 2014  Davide Mottin (mottin@disi.unitn.eu
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

package it.unitn.disi.db.queryrelaxation.model;

import java.util.Comparator;

/**
 * Class to represent a comparator for a pair that compairs the second member
 * of that pair
 * @author Davide Mottin
 */
public class PairSecondComparator implements Comparator<Pair<?, ? extends Comparable>> {
    private boolean descent = false;

    public PairSecondComparator() {
        this(false);
    }

    public PairSecondComparator(boolean descent) {
        this.descent = descent;
    }

    @Override
    public int compare(Pair<?, ? extends Comparable> o1, Pair<?, ? extends Comparable> o2) {
        int value = o1.getSecond().compareTo(o2.getSecond());
        if (descent)
            value = -value;
        return value;
    }

}
