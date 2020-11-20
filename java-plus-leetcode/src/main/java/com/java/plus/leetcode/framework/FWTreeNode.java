package com.java.plus.leetcode.framework;

import com.java.plus.leetcode.model.TreeNode;

/**
 * 二叉树遍历框架，典型的非线性递归遍历结构
 * */
public class FWTreeNode {
	
	public void traverse(TreeNode root) {
	    traverse(root.left);
	    traverse(root.right);
	}
	
	// 定义：count(root) 返回以 root 为根的树有多少节点
	public static int count(TreeNode root) {
	    // base case
	    if (root == null) return 0;
	    // 自己加上子树的节点数就是整棵树的节点数
	    return 1 + count(root.left) + count(root.right);
	}

}
