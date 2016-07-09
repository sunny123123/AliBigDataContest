package com.sunny.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class OperateFileOld {
	private static BufferedReader br = null;
	private static BufferedWriter bw = null;
	
	static{
		String path = System.getProperty("user.dir");
		File file = new File("./rs.txt");
		 try {
			bw = new BufferedWriter(new FileWriter(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static synchronized BufferedWriter getWriter(BufferedWriter bw,String fileName){
		if(bw==null){
			File file = new File(fileName);
			 try {
				//br = new BufferedReader(new FileReader(file));
				bw = new BufferedWriter(new FileWriter(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return bw;
	}
	public static void writeContent(BufferedWriter bw,String context){
		try {
			bw.write(context);
			bw.newLine();
			bw.flush();
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
			e.printStackTrace();
		}
		
		
	}
}
