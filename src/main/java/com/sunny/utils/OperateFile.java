package com.sunny.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.alibaba.middleware.race.RaceConfig;

public class OperateFile {
	//private static BufferedReader br = null;
	private static BufferedWriter bw = null;
	//private static final String dir = "/BBBB/jstorm-2.1.1/logs/"+RaceConfig.JstormTopologyName+"/";
	private static final String dir = "./"+RaceConfig.JstormTopologyName+"/";
	public static synchronized BufferedWriter getWriter(String fileName){
		
			File file = new File(dir+fileName);
			if(!file.exists()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			 try {
				//br = new BufferedReader(new FileReader(file));
				bw = new BufferedWriter(new FileWriter(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
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
	/*public static void clearDir(){
		try {
			FileUtils.cleanDirectory(new File(dir));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}*/
	
}
