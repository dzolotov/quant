package org.gathe.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by zolotov on 28.11.13.
 */
public class DataClass {
        private String className;
        private String extend = null;
        private boolean readOnly = false;
        private boolean canSpecify = false;
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
}
