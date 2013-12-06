package org.gathe.integration;

import org.w3c.dom.Document;

import java.util.HashMap;
import java.util.List;

/**
        This program is free software: you can redistribute it and/or modify
        it under the terms of the GNU General Public License as published by
        the Free Software Foundation, either version 3 of the License, or
        (at your option) any later version.

        This program is distributed in the hope that it will be useful,
        but WITHOUT ANY WARRANTY; without even the implied warranty of
        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
        GNU General Public License for more details.

        You should have received a copy of the GNU General Public License
        along with this program.  If not, see <http://www.gnu.org/licenses/>.

        @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>
        @Description Accessor base interface
        */
public interface Accessor {

    public void setConnector(Connector connector);

    //get endpoint schema
    public List<DataClass> getSchema();

    //get object description from endpoint
    //public Document get(String transactionId, String className, String uuid);
    public boolean get(String className, GetHelper helper);

    //update object description from endpoint
    //public boolean update(String transactionId, String className, String uuid, String data);
    public boolean update(String className, UpdateHelper helper);

    //remove object from endpoint
    public boolean remove(String transactionId, String className, String uuid);

    //get UUID by identifier (seek)
    public String getUuidByIdentifier(String transactionId, String className, String identifierName, String identifierValue);

    //resolve identifier by UUID (reverse-seek)
    public String getIdentifierByUuid(String transactionId, String className, String identifierName, String uuidValue);

    //check presence by identifier
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue);

    //perform specification for subclass of generic classname
    public String specify(String transactionId, String genericClassName, String uuid);

    //perform data validation before request
    public String[] validate(String className, UpdateHelper helper);

    //perform data backup before remove (or as scheduled task)
    public boolean backup(String transactionId, String className, String uuid);

}
