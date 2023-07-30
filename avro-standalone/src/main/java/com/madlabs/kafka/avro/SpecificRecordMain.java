package com.madlabs.kafka.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.example.Customer;

public class SpecificRecordMain {

	public static void main(String[] args) {
		Customer.Builder builder = Customer.newBuilder();
		builder.setFirstName("Jim");
		builder.setLastName("Jhonston");
		builder.setAge(32);
		builder.setHeight(192.3f);
		builder.setWeight(85);
		Customer customer = builder.build();
		System.out.println(customer);

		final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<Customer>(Customer.class);
		try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<Customer>(datumWriter)) {
			dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
			dataFileWriter.append(customer);
			System.out.println("Written customer-specific.avro");
		} catch (IOException exe) {
			System.out.println("Couldnt write");
			exe.printStackTrace();
		}

		// 3 read specific record from file
		final File file = new File("customer-specific.avro");
		final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
		final DataFileReader<Customer> dataFileReader;
		try {
			System.out.println("Reading specific record!!");
			dataFileReader = new DataFileReader<>(file, datumReader);
			while (dataFileReader.hasNext()) {
				Customer customerRecord = dataFileReader.next();
				System.out.println(customerRecord.toString());
				System.out.println(customerRecord.getFirstName());
				//System.out.println(customerRecord.get("sad"));  AvroRuntimeException: Not a valid schema field: sad
			}
		} catch (IOException exe) {
			System.out.println("Couldnt read");
			exe.printStackTrace();
		}

	}
}
