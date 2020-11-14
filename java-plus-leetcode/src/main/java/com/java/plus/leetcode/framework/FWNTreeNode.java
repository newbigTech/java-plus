package com.java.plus.leetcode.framework;

/**
 * 二叉树框架可以扩展为 N 叉树的遍历框架
 * */
public class FWNTreeNode {

	public void traverse(TreeNode root) {
	    for (TreeNode child : root.children)
	        traverse(child);
	}

	/* 基本的 N 叉树节点 */
	public class TreeNode {
	    int val;
	    TreeNode[] children;
	}
	
}


