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


import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Representation of a grouping of cells. May contain cells from more than
 * one row. Encapsulates RowModel and CellModel models.
 * <p/>
 * <pre>
 * &lt;complexType name="CellSet"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="row" type="tns:Row" maxOccurs="unbounded"
 *       minOccurs="1"&gt;&lt;/element&gt;
 *   &lt;/sequence&gt;
 * &lt;/complexType&gt;
 * <p/>
 * &lt;complexType name="Row"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="key" type="base64Binary"&gt;&lt;/element&gt;
 *     &lt;element name="cell" type="tns:Cell"
 *       maxOccurs="unbounded" minOccurs="1"&gt;&lt;/element&gt;
 *   &lt;/sequence&gt;
 * &lt;/complexType&gt;
 * <p/>
 * &lt;complexType name="Cell"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="value" maxOccurs="1" minOccurs="1"&gt;
 *       &lt;simpleType&gt;
 *         &lt;restriction base="base64Binary"/&gt;
 *       &lt;/simpleType&gt;
 *     &lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="column" type="base64Binary" /&gt;
 *   &lt;attribute name="timestamp" type="int" /&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name = "CellSet")
public class CellSetModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<RowModel> rows;

    /**
     * Constructor
     */
    public CellSetModel() {
        this.rows = new ArrayList<RowModel>();
    }

    /**
     * @param rows the rows
     */
    public CellSetModel(List<RowModel> rows) {
        super();
        this.rows = rows;
    }

    /**
     * Add a row to this cell set
     *
     * @param row the row
     */
    public void addRow(RowModel row) {
        rows.add(row);
    }

    /**
     * @return the rows
     */
    @XmlElement(name = "Row")
    public List<RowModel> getRows() {
        return rows;
    }


}
