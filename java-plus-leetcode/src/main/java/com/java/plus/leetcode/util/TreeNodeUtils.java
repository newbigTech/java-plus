package com.java.plus.leetcode.util;

import com.java.plus.leetcode.model.TreeNode;

public class TreeNodeUtils {
	/**
	 * 创建一个简单的链表{1,2,3,4,5,...,9}
	 * */
	public static TreeNode createNode() {
		int[] nums= {3,2,1,6,0,5};
		return constructMaximumBinaryTree(nums);
	}
	
	/* 主函数 */
	static TreeNode constructMaximumBinaryTree(int[] nums) {
	    return build(nums, 0, nums.length - 1);
	}
	
	/* 将 nums[lo..hi] 构造成符合条件的树，返回根节点 */
	static TreeNode build(int[] nums, int lo, int hi) {
	    // base case
	    if (lo > hi) {
	        return null;
	    }

	    // 找到数组中的最大值和对应的索引
	    int index = -1, maxVal = Integer.MIN_VALUE;
	    for (int i = lo; i <= hi; i++) {
	        if (maxVal < nums[i]) {
	            index = i;
	            maxVal = nums[i];
	        }
	    }

	    TreeNode root = new TreeNode(maxVal);
	    // 递归调用构造左右子树
	    root.left = build(nums, lo, index - 1);
	    root.right = build(nums, index + 1, hi);

	    return root;
	}
	
	

	// 为了便于查看结果，写的打印链表的方法
	public static void print(TreeNode head) {
		while (head != null) { 
			System.out.print(head.val + "->"); 
			head = head.right;
		}
		System.out.println();
	}
	
	/**
	 * 序列化和反序列化二叉树 
	 * */
	public static String traverse(TreeNode root) {
	    // 对于空节点，可以用一个特殊字符表示
	    if (root == null) {
	        return "#";
	    }
	    // 将左右子树序列化成字符串
	    String left = traverse(root.left);
	    String right = traverse(root.right);
	    /* 后序遍历代码位置 */
	    // 左右子树加上自己，就是以自己为根的二叉树序列化结果
	    String subTree = left + "," + right + "," + root.val;
	    return subTree;
	}
}
