package com.cottagecoders.postgreswalreader;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Wal2JsonRecord {

  @SerializedName("xid")
  @Expose
  private long xid;
  @SerializedName("change")
  @Expose
  private List<Change> change = null;

  /**
   * No args constructor for use in serialization
   */
  public Wal2JsonRecord() {
  }

  /**
   * @param xid
   * @param change
   */
  public Wal2JsonRecord(long xid, List<Change> change) {
    super();
    this.xid = xid;
    this.change = change;
  }

  public long getXid() {
    return xid;
  }

  public void setXid(long xid) {
    this.xid = xid;
  }

  public List<Change> getChange() {
    return change;
  }

  public void setChange(List<Change> change) {
    this.change = change;
  }

}
