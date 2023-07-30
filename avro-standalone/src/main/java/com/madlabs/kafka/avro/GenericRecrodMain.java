package com.madlabs.kafka.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecrodMain {

	public static void main(String[] args) {

		// 1 Define Schema
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse("{\n" + "     \"type\": \"record\",\n" + "     \"namespace\": \"com.example\",\n"
				+ "     \"name\": \"Customer\",\n" + "     \"fields\": [\n"
				+ "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n"
				+ "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n"
				+ "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n"
				+ "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n"
				+ "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n"
				+ "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n"
				+ "     ]\n" + "}");

		// 2 create generic record
		GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
		customerBuilder.set("first_name", "John");
		customerBuilder.set("last_name", "Doe");
		customerBuilder.set("age", 25);
		customerBuilder.set("height", 170f);
		customerBuilder.set("weight", 80.5f);
		customerBuilder.set("automated_email", false);
		GenericData.Record customer = customerBuilder.build();

		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
			dataFileWriter.append(customer);
			System.out.println("Written customer-generic.avro");
		} catch (IOException exe) {
			System.out.println("Couldnt write");
			exe.printStackTrace();
		}

		// 3 read generic record from file
		final File file = new File("customer-generic.avro");
		final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		GenericRecord customerRecord;
		try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)) {
			customerRecord = dataFileReader.next();
			System.out.println("Read avro file!!");
			System.out.println(customerRecord.toString());
			System.out.println(customerRecord.get("first_name"));
			System.out.println(customerRecord.get("asd"));
		} catch (IOException exe) {

		}

	}

}
