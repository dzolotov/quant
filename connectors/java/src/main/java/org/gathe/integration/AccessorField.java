package org.gathe.integration;

import java.util.List;

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
 */

public abstract class AccessorField {
    public abstract boolean isIdentifier();

    public abstract String getPath();

    public abstract String getId();

    public abstract String getDefault();

    public abstract List<ReplaceJAXB> getReplaces();

    public abstract String getScope();

    public abstract String getDescription();

    public abstract String getRef();

    public abstract String getKey();

    public abstract String getType();

    public abstract String getNullBehavior();

    public abstract String getEmptyBehavior();

    public abstract List<AppendJAXB> getAppends();
}

