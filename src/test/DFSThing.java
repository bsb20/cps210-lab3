package test;

import common.DFileID;

import dfs.DFS;

public class DFSThing
{

    public static void main(String[] args)
    {
		System.out.println("Making DFS");
        DFS dfs = new DFS(true);
        dfs.init();
		System.out.println("Making file");
		DFileID dfid = dfs.createDFile();
		System.out.println(dfs.listAllDFiles().size());
    }

}
