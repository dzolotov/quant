package org.gathe.integration;


import java.util.ArrayList;
import java.util.Iterator;

/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
 * @Description Class schema description
 */
public class DataClass {
    private String className;
    private String extend = null;
    private boolean matchable = false;

    public boolean isMatchable() {
        return matchable;
    }

    public void setMatchable(boolean matchable) {
        this.matchable = matchable;
    }

    private boolean readOnly = false;
    private boolean canSpecify = false;
    private boolean monolithic = false;
    private ArrayList<String> identifiers = new ArrayList<>();
    private ArrayList<String> checks = new ArrayList<>();
    private ArrayList<DataElement> classElements = new ArrayList<>();

    public DataClass(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public String getExtendClassName() {
        return extend;
    }

    public DataClass(String className, DataClass extendOf) {
        this.className = className;
        extend = extendOf.getClassName();
    }

    public DataClass addIdentifier(String identifier) {
        identifiers.add(identifier);
        return this;
    }

    public DataClass addCheck(String checkId) {
        checks.add(checkId);
        return this;
    }

    public DataClass addElement(DataElement element) {
        classElements.add(element);
        return this;
    }

    public Iterator<String> getIdentifiers() {
        return identifiers.iterator();
    }

    public Iterator<String> getChecks() {
        return checks.iterator();
    }

    public Iterator<DataElement> getElements() {
        return classElements.iterator();
    }


    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }

    public void setSpecifiability(boolean specifiability) {
        this.canSpecify = specifiability;
    }

    public boolean isSpecifiability() {
        return canSpecify;
    }

    public void setMonolithic(boolean monolithic) {
        this.monolithic = monolithic;
    }

    public boolean isMonolithic() {
        return (this.monolithic);
    }
}
