package com.java.plus.leetcode.framework;

import com.java.plus.leetcode.model.ListNode;
import com.java.plus.leetcode.util.Utils;

/**
 * 链表遍历框架，兼具迭代和递归结构
 */
public class FWListNode {
	
	public static void main(String[] args) {
		ListNode head=Utils.createListNode();
		Utils.printListNode(head);
		ListNode newhead=reverse(head);

		Utils.printListNode(newhead);
	}

	/**
	 * 递归反转整个链表
	 * 递归算法:输入一个节点 head，将「以 head 为起点」的链表反转，并返回反转之后的头结点。
	 * */
	public static ListNode reverse(ListNode head) {
		if (head.next == null)
			return head;
		ListNode last = reverse(head.next);
		head.next.next = head; 
		head.next = null;//避免1->2和2->1
		return last;
	}

	public void traverse(ListNode head) {
		for (ListNode p = head; p != null; p = p.next) {
			// 迭代访问 p.val
		}
	}

//	public void traverse(ListNode head) {
//	    // 递归访问 head.val
//	    traverse(head.next);
//	} 

}
