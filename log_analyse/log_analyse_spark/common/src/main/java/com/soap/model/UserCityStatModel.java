package com.soap.model;

public class UserCityStatModel extends BaseModel
{
  private static final long serialVersionUID = 123456789L;
  private String userId;
  private String city;

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getUserId() {
    return userId;
  }

}
