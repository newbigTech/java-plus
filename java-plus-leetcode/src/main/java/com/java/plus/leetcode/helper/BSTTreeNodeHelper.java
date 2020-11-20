package com.java.plus.leetcode.helper;

import com.java.plus.leetcode.model.TreeNode;

public class BSTTreeNodeHelper {

	/**
	 * 在二叉树递归框架之上，扩展出一套 BST 代码框架
	 * */
	public static void BST(TreeNode root, int target) {
	    if (root.val == target)
	        // 找到目标，做点什么
	    if (root.val < target) 
	        BST(root.right, target);
	    if (root.val > target)
	        BST(root.left, target);
	}
}
