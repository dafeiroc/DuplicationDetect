/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yottabox.hbasemodel;


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import javax.xml.bind.annotation.*;
import javax.xml.namespace.QName;
import java.io.Serializable;
import java.util.*;

/**
 * A representation of HBase table descriptors.
 * <p/>
 * <pre>
 * &lt;complexType name="TableSchema"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="column" type="tns:ColumnSchema"
 *       maxOccurs="unbounded" minOccurs="1"&gt;&lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="name" type="string"&gt;&lt;/attribute&gt;
 *   &lt;anyAttribute&gt;&lt;/anyAttribute&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name = "TableSchema")
@XmlType(propOrder = {"name", "columns"})
public class TableSchemaModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final QName IS_META = new QName(HTableDescriptor.IS_META);
    private static final QName IS_ROOT = new QName(HTableDescriptor.IS_ROOT);
    private static final QName READONLY = new QName(HTableDescriptor.READONLY);
    private static final QName TTL = new QName(HColumnDescriptor.TTL);
    private static final QName VERSIONS = new QName(HConstants.VERSIONS);
    private static final QName COMPRESSION =
            new QName(HColumnDescriptor.COMPRESSION);

    private String name;
    private Map<QName, Object> attrs = new HashMap<QName, Object>();
    private List<ColumnSchemaModel> columns = new ArrayList<ColumnSchemaModel>();

    /**
     * Default constructor.
     */
    public TableSchemaModel() {
    }

    /**
     * Constructor
     *
     * @param htd the table descriptor
     */
    public TableSchemaModel(HTableDescriptor htd) {
        setName(htd.getNameAsString());
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e :
                htd.getValues().entrySet()) {
            addAttribute(Bytes.toString(e.getKey().get()),
                    Bytes.toString(e.getValue().get()));
        }
        for (HColumnDescriptor hcd : htd.getFamilies()) {
            ColumnSchemaModel columnModel = new ColumnSchemaModel();
            columnModel.setName(hcd.getNameAsString());
            for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e :
                    hcd.getValues().entrySet()) {
                columnModel.addAttribute(Bytes.toString(e.getKey().get()),
                        Bytes.toString(e.getValue().get()));
            }
            addColumnFamily(columnModel);
        }
    }

    /**
     * Add an attribute to the table descriptor
     *
     * @param name  attribute name
     * @param value attribute value
     */
    public void addAttribute(String name, Object value) {
        attrs.put(new QName(name), value);
    }

    /**
     * Return a table descriptor value as a string. Calls toString() on the
     * object stored in the descriptor value map.
     *
     * @param name the attribute name
     * @return the attribute value
     */
    public String getAttribute(String name) {
        Object o = attrs.get(new QName(name));
        return o != null ? o.toString() : null;
    }

    /**
     * Add a column family to the table descriptor
     *
     * @param family the column family model
     */
    public void addColumnFamily(ColumnSchemaModel family) {
        columns.add(family);
    }

    /**
     * Retrieve the column family at the given index from the table descriptor
     *
     * @param index the index
     * @return the column family model
     */
    public ColumnSchemaModel getColumnFamily(int index) {
        return columns.get(index);
    }

    /**
     * @return the table name
     */
    @XmlAttribute
    public String getName() {
        return name;
    }

    /**
     * @return the map for holding unspecified (user) attributes
     */
    @XmlAnyAttribute
    public Map<QName, Object> getAny() {
        return attrs;
    }

    /**
     * @return the columns
     */
    @XmlElement(name = "ColumnSchema")
    public List<ColumnSchemaModel> getColumns() {
        return columns;
    }

    /**
     * @param name the table name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @param columns the columns to set
     */
    public void setColumns(List<ColumnSchemaModel> columns) {
        this.columns = columns;
    }

    /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ NAME=> '");
        sb.append(name);
        sb.append('\'');
        for (Map.Entry<QName, Object> e : attrs.entrySet()) {
            sb.append(", ");
            sb.append(e.getKey().getLocalPart());
            sb.append(" => '");
            sb.append(e.getValue().toString());
            sb.append('\'');
        }
        sb.append(", COLUMNS => [ ");
        Iterator<ColumnSchemaModel> i = columns.iterator();
        while (i.hasNext()) {
            ColumnSchemaModel family = i.next();
            sb.append(family.toString());
            if (i.hasNext()) {
                sb.append(',');
            }
            sb.append(' ');
        }
        sb.append("] }");
        return sb.toString();
    }

    // getters and setters for common schema attributes

    // cannot be standard bean type getters and setters, otherwise this would
    // confuse JAXB

    /**
     * @return true if IS_META attribute exists and is truel
     */
    public boolean __getIsMeta() {
        Object o = attrs.get(IS_META);
        return o != null ? Boolean.valueOf(o.toString()) : false;
    }

    /**
     * @return true if IS_ROOT attribute exists and is truel
     */
    public boolean __getIsRoot() {
        Object o = attrs.get(IS_ROOT);
        return o != null ? Boolean.valueOf(o.toString()) : false;
    }

    /**
     * @return true if READONLY attribute exists and is truel
     */
    public boolean __getReadOnly() {
        Object o = attrs.get(READONLY);
        return o != null ?
                Boolean.valueOf(o.toString()) : HTableDescriptor.DEFAULT_READONLY;
    }

    /**
     * @param value desired value of IS_META attribute
     */
    public void __setIsMeta(boolean value) {
        attrs.put(IS_META, Boolean.toString(value));
    }

    /**
     * @param value desired value of IS_ROOT attribute
     */
    public void __setIsRoot(boolean value) {
        attrs.put(IS_ROOT, Boolean.toString(value));
    }

    /**
     * @param value desired value of READONLY attribute
     */
    public void __setReadOnly(boolean value) {
        attrs.put(READONLY, Boolean.toString(value));
    }


    /**
     * @return a table descriptor
     */
    public HTableDescriptor getTableDescriptor() {
        HTableDescriptor htd = new HTableDescriptor(getName());
        for (Map.Entry<QName, Object> e : getAny().entrySet()) {
            htd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
        }
        for (ColumnSchemaModel column : getColumns()) {
            HColumnDescriptor hcd = new HColumnDescriptor(column.getName());
            for (Map.Entry<QName, Object> e : column.getAny().entrySet()) {
                hcd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
            }
            htd.addFamily(hcd);
        }
        return htd;
    }

}
