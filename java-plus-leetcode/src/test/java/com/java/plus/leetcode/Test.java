package com.java.plus.leetcode;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.java.plus.leetcode.model.TreeNode;
import com.java.plus.leetcode.util.TreeNodeUtils;;

public class Test {

	public static void main(String[] args) {
		TreeNode	head=TreeNodeUtils.createNode();
		traverse(head);
	    System.out.println();
		System.out.println(count(head));
	}
	/* 二叉树遍历框架 */
	public static 	void traverse(TreeNode root) {
		if(root!=null) {
	    // 前序遍历   访问顺序：先根节点，再左子树，最后右子树
//		System.out.print(root.val+"->");
	    traverse(root.left);
	    // 中序遍历   访问顺序：先左子树，再根节点，最后右子树
		System.out.print(root.val+"-->");
	    traverse(root.right);
	    // 后序遍历  访问顺序：先左子树，再右子树，最后根节点
//		System.out.print(root.val+"--->");
	
		}
	}
	
	// 定义：count(root) 返回以 root 为根的树有多少节点
	public static int count(TreeNode root) {
	    // base case
	    if (root == null) return 0;
	    // 自己加上子树的节点数就是整棵树的节点数
	    return 1 + count(root.left) + count(root.right);
	}
	
	
}
