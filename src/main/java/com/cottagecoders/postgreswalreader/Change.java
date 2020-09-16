package com.cottagecoders.postgreswalreader;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

class Change {

  @SerializedName("kind")
  @Expose
  private String kind;
  @SerializedName("schema")
  @Expose
  private String schema;
  @SerializedName("table")
  @Expose
  private String table;
  @SerializedName("columnnames")
  @Expose
  private List<String> columnnames = null;
  @SerializedName("columntypes")
  @Expose
  private List<String> columntypes = null;
  @SerializedName("columnvalues")
  @Expose
  private List<String> columnvalues = null;

  /**
   * No args constructor for use in serialization
   */
  public Change() {
  }

  /**
   * @param schema
   * @param kind
   * @param columntypes
   * @param columnvalues
   * @param columnnames
   * @param table
   */
  public Change(
      String kind,
      String schema,
      String table,
      List<String> columnnames,
      List<String> columntypes,
      List<String> columnvalues
  ) {
    super();
    this.kind = kind;
    this.schema = schema;
    this.table = table;
    this.columnnames = columnnames;
    this.columntypes = columntypes;
    this.columnvalues = columnvalues;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<String> getColumnnames() {
    return columnnames;
  }

  public void setColumnnames(List<String> columnnames) {
    this.columnnames = columnnames;
  }

  public List<String> getColumntypes() {
    return columntypes;
  }

  public void setColumntypes(List<String> columntypes) {
    this.columntypes = columntypes;
  }

  public List<String> getColumnvalues() {
    return columnvalues;
  }

  public void setColumnvalues(List<String> columnvalues) {
    this.columnvalues = columnvalues;
  }

}
