package com.sunny.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class OperateFile {
	private static BufferedReader br = null;
	private static BufferedWriter bw = null;
	static{
		String path = System.getProperty("user.dir");
		//File file = new File("/root/workspace/AliBigDataContest/rs.txt");
		File file = new File("./rs.txt");
		 try {
			//br = new BufferedReader(new FileReader(file));
			bw = new BufferedWriter(new FileWriter(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void writeToFile(String context){
		try {
			bw.write(context);
			bw.newLine();
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
