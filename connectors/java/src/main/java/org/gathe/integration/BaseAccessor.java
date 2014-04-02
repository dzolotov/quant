package org.gathe.integration;

import java.util.ArrayList;
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

 @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
 */

public abstract class BaseAccessor extends Thread implements Accessor {
    protected Connector connector;

    @Override
    public String[] match(String transactionId, String className, HashMap<String, String> filters, boolean explain) {
        return new String[0];
    }

    @Override
    public List<DataClass> getSchema() {
        return new ArrayList<DataClass>();
    }

    @Override
    public boolean get(String className, GetHelper helper) {
        return false;
    }

    @Override
    public boolean update(String className, UpdateHelper helper) {
        return false;
    }

    @Override
    public boolean remove(String transactionId, String className, String uuid) {
        return false;
    }

    @Override
    public String getUuidByIdentifier(String transactionId, String className, String identifierName, String identifierValue, boolean forcedCreation) {
        return "";
    }

    @Override
    public String getIdentifierByUuid(String transactionId, String className, String identifierName, String uuidValue) {
        return "";
    }

    @Override
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue) {
        return false;
    }

    @Override
    public String specify(String transactionId, String genericClassName, String uuid) {
        return "";
    }

    @Override
    public String[] validate(String className, UpdateHelper helper) {
        return new String[0];
    }

    @Override
    public boolean backup(String transactionId, String className, String uuid) {
        return false;
    }

    public void setConnector(Connector connector) {

        this.connector = connector;
    }

    @Override
    public boolean isModified(String transactionId, String className, String uuid) {
        return false;
    }

    @Override
    public String countMatches(String transactionId, String className, HashMap<String, String> filters) {
        return "0/0";
    }
}