package com.java.plus.leetcode.framework;

/**
 * 二叉树遍历框架，典型的非线性递归遍历结构
 * */
public class FWTreeNode {
	
	public void traverse(TreeNode root) {
	    traverse(root.left);
	    traverse(root.right);
	}
	
	/** 
	 * 基本的二叉树节点
	 * */
	public class TreeNode {
	    int val;
	    TreeNode left, right;
	}

}
