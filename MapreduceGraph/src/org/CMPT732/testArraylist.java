package org.CMPT732;

import java.util.ArrayList;

public class testArraylist {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 ArrayList<Integer> path = new ArrayList<Integer>();
		 path.add(2);
		 path.add(3);
		 String newin = "2  3";
		 String[] a = newin.split("\\s+");
		 System.out.println(a[0]+a[1]);

	}

}
