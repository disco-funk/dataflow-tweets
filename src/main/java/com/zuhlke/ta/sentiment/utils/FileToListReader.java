package com.zuhlke.ta.sentiment.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FileToListReader {
	public static List<String> readFile(String fileName) {
		List<String> out = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(FileToListReader.class.getClassLoader().getResourceAsStream(fileName)));
			String line = null;
			while((line = reader.readLine()) != null)
				out.add(line);
			return out;
		} catch (IOException e) {
			return out;
		}
	}
}
