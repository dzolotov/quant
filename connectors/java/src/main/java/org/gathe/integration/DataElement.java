package org.gathe.integration;

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
public class DataElement {
    private final String path;
    private final String description;
    private final boolean required;

    public DataElement(String path,String description) {
        this.path = path;
        this.description = description;
        required = false;
    }

    public DataElement(String path,String description, boolean required) {
        this.path = path;
        this.description = description;
        this.required = required;
    }

    public String getXPath() {
        return this.path;
    }

    public String getDescription() {
        return this.description;
    }

    public boolean isRequired() {
        return this.required;
    }
}

