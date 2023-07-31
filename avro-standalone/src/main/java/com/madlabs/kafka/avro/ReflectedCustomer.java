package com.madlabs.kafka.avro;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;

public class ReflectedCustomer {

	private String firstName;
	private String lastName;
	@Nullable
	private String nickName;

	private Address primaryAddress;

	public ReflectedCustomer() {
	}

	public ReflectedCustomer(String firstName, String lastName, String nickName) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.nickName = nickName;
	}

	public ReflectedCustomer(String firstName, String lastName, String nickName, Address primaryAddress) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.nickName = nickName;
		this.primaryAddress = primaryAddress;
	}

	public String fullName() {
		return this.firstName + " " + this.lastName + " " + this.nickName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getNickName() {
		return nickName;
	}

	public void setNickName(String nickName) {
		this.nickName = nickName;
	}

	public Address getPrimaryAddress() {
		return primaryAddress;
	}

	public void setPrimaryAddress(Address primaryAddress) {
		this.primaryAddress = primaryAddress;
	}

}

class Address {
	private String addressLine1;
	@Nullable
	private String addressLine2;

	private String city;

	@Nullable
	private String postCode;

	public Address() {
		super();
	}

	public Address(String addressLine1, String addressLine2, String city, String postCode) {
		super();
		this.addressLine1 = addressLine1;
		this.addressLine2 = addressLine2;
		this.city = city;
		this.postCode = postCode;
	}

	public Address(String addressLine1, String addressLine2, String city) {
		super();
		this.addressLine1 = addressLine1;
		this.addressLine2 = addressLine2;
		this.city = city;
	}

	public String getAddressLine1() {
		return addressLine1;
	}

	public void setAddressLine1(String addressLine1) {
		this.addressLine1 = addressLine1;
	}

	public String getAddressLine2() {
		return addressLine2;
	}

	public void setAddressLine2(String addressLine2) {
		this.addressLine2 = addressLine2;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getPostCode() {
		return postCode;
	}

	public void setPostCode(String postCode) {
		this.postCode = postCode;
	}

}
