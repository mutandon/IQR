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

package it.unitn.disi.db.queryrelaxation.model;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;

/**
 *
 * @author Alice Marascu
 */
public class VectorRandomQuery {

    public Vector query_elem; //keeps Integers

    public VectorRandomQuery(){
        this.query_elem = new Vector();
    }

    public boolean haveSameValues(VectorRandomQuery v){

        if(v.query_elem.size() != this.query_elem.size())
            return false;
        
        for(int i = 0; i < this.query_elem.size(); i++)
            if(!v.query_elem.contains( (Integer) this.query_elem.get(i) ))
                return false;
        
        return true;
        
    }

    public void sortTheValues(){
        java.util.Collections.sort(this.query_elem);

    }

    public String toString(){
        String tmp = "";
        for(int i = 0; i< this.query_elem.size(); i++)
            tmp += (Integer)this.query_elem.get(i)+ " ";
        return tmp;
    }

    public List<Constraint> transf(){
        List<Constraint> cons= new ArrayList<Constraint>();

        for(int i = 0; i < this.query_elem.size(); i++)
            cons.add(new Constraint( ((Integer)this.query_elem.get(i))+"", true));
        
        return cons;
    }
}
