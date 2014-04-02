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


public class ResponseThread extends Thread {
    private AsyncAccessor accessor;
    private String messageId;
    private String result;

    public ResponseThread(AsyncAccessor accessor, String messageId, String result) {
        this.accessor = accessor;
        this.messageId = messageId;
        this.result = result;
    }

    public void run() {
        this.accessor.processResult(this.messageId, this.result);
    }
}
