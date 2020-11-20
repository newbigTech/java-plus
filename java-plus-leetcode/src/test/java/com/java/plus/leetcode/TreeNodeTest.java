package com.java.plus.leetcode;

import com.java.plus.leetcode.framework.FWTreeNode;
import com.java.plus.leetcode.model.TreeNode;
import com.java.plus.leetcode.util.TreeNodeUtils;

public class TreeNodeTest {

	public static void main(String[] args) {
		TreeNode node = TreeNodeUtils.createNode();
		TreeNodeUtils.print(node);
		
		int count=FWTreeNode.count(node);
		
		System.out.println(count);
		
		String str=	TreeNodeUtils.traverse(node);
		System.out.println(str);
	}
	
	
	// 定义：count(root) 返回以 root 为根的树有多少节点
	int count(TreeNode root) {
	    // base case
	    if (root == null) return 0;
	    // 自己加上子树的节点数就是整棵树的节点数
	    return 1 + count(root.left) + count(root.right);
	}
	
	
	
}
