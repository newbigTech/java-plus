package com.java.plus.leetcode.util;

import com.java.plus.leetcode.model.ListNode;

public class Utils {
	
	/**
	 * 创建一个简单的链表{1,2,3,4,5,...,9}
	 * */
	public static ListNode createListNode() {
		ListNode head = new ListNode(1);//创建头节点
		head.next = new ListNode(2);//再定义头节点的next域
		ListNode t = head.next;
		for(int i=3;i<10;i++) {//创建一个简单的链表{1,2,3,4,5,...,9}
			t.next = new ListNode(i);
			t = t.next;
		}
		return head;
	}

	// 为了便于查看结果，写的打印链表的方法
	public static void printListNode(ListNode head) {
		while (head != null) { 
			System.out.print(head.val + "->"); 
			head = head.next;
		}
		System.out.println();
	}
}
